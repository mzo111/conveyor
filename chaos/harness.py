"""Chaos harness: real worker processes, real SIGKILLs, assertions from durable rows.

What it attacks
===============
``conveyor.queue`` promises at-least-once delivery and names the window that
breaks anything stronger: between a handler's side effect committing and the
ack committing. This harness kills worker processes inside that window (and
outside it, and while a lease expires under a still-running handler), runs the
reaper on a timer, restarts the dead workers, and then checks the ledger.

Assertions (all computed from ``jobs``, ``chaos_effects``, ``chaos_executions``):

* every enqueued job is terminal (``succeeded`` or ``dead``);
* no job is dead unless its payload says it must fail; every must-fail job is
  dead with zero effects;
* zero lost: every non-failing job has at least one effect row;
* ``idempotent`` mode: every non-failing job has exactly one effect row, and the
  window was actually hit (some execution reached the effect phase more than
  once for the same job);
* ``naive`` mode: at least one job has more than one effect row. If the naive
  handler shows no duplicates, the harness never exercised the window and the
  idempotent result is worthless, so that is a failure too.

Reproducibility
===============
``seed`` fixes every payload (sleep durations, which jobs fail, which overrun
their lease) and every kill decision: for each ``(job_id, attempt)`` the kill
verdict, timing and delay come from ``random.Random(f"{seed}:{job_id}:{attempt}")``.
OS scheduling still varies, so the same seed reproduces the harness's
*decisions*, not the exact wall-clock interleaving. A failure report therefore
prints the interleaving that actually happened, from the ledger, rather than
relying on a re-run.

Targeting rule, stated because it looks like a weakened assertion but is not:
the harness never kills an execution whose attempt number is within the last
two permitted. A job can only reach ``dead`` by its own failure, so
``unexpected_dead`` really means the queue lost something.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import random
import signal
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import DictRow

from conveyor.db import connect, dsn_from_env
from conveyor.queue import enqueue
from conveyor.reaper import reap_expired

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
SCHEMA = Path(__file__).resolve().parent / "schema.sql"
KILL_TIMINGS = ("random", "before_effect", "after_effect", "mixed")
MODES = ("idempotent", "naive")
TERMINAL = ("succeeded", "dead")


@dataclass
class ChaosConfig:
    mode: str = "idempotent"
    workers: int = 6
    jobs: int = 200
    kill_probability: float = 0.3
    kill_timing: str = "random"
    duration: float = 120.0  # max wall-clock seconds; non-terminal after this is a failure
    seed: int | None = None
    visibility_timeout: float = 2.0
    reaper_interval: float = 0.25
    max_sleep: float = 0.3  # pre/post handler sleeps drawn uniform(0, max_sleep)
    fail_probability: float = 0.05
    overrun_probability: float = 0.03  # jobs whose post sleep outlives the lease
    max_attempts: int = 8
    restart_delay: float = 0.1
    # Off by default so the overrun jobs really do outlive their lease and force
    # the reaper to requeue a job whose handler is still running (docstring case
    # 2 in conveyor.queue). With heartbeating on, only kills exercise the window.
    heartbeat: bool = False
    log_dir: str | None = None  # worker stderr goes here; default: a temp dir
    dsn: str | None = None

    def validate(self) -> None:
        if self.mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}")
        if self.kill_timing not in KILL_TIMINGS:
            raise ValueError(f"kill_timing must be one of {KILL_TIMINGS}")
        if self.max_attempts < 3:
            raise ValueError("max_attempts must be >= 3 so the last two attempts are never killed")
        if not 0 <= self.kill_probability <= 1:
            raise ValueError("kill_probability must be in [0, 1]")


@dataclass
class Kill:
    t: float  # host epoch seconds
    worker: str
    pid: int
    job_id: int
    attempt: int
    timing: str  # before_effect | after_effect | random
    phase_seen: str  # last phase line read from the worker before the kill


@dataclass
class Miss:
    t: float
    worker: str
    job_id: int
    attempt: int
    timing: str
    reason: str


@dataclass
class Reap:
    t: float  # DB clock_timestamp() epoch seconds
    job_id: int
    attempt: int
    status: str


@dataclass
class WorkerExit:
    t: float
    worker: str
    pid: int
    returncode: int
    killed_by_us: bool


@dataclass
class Violation:
    kind: str
    message: str
    job_id: int | None = None


@dataclass
class RunResult:
    config: ChaosConfig
    run_id: str
    seed: int
    started_at: float
    wall_seconds: float
    stats: dict[str, Any]
    violations: list[Violation]
    timelines: dict[int, list[str]] = field(default_factory=dict)
    log_dir: str = ""

    @property
    def ok(self) -> bool:
        return not self.violations

    def summary(self) -> str:
        s = self.stats
        lines = [
            f"run {self.run_id}  mode={self.config.mode}  seed={self.seed}  "
            f"workers={self.config.workers}  kill_timing={self.config.kill_timing}  "
            f"kill_probability={self.config.kill_probability}",
            f"  jobs enqueued          {s['enqueued']}",
            f"  completed (succeeded)  {s['succeeded']}",
            f"  dead                   {s['dead']}  (expected to fail: {s['expected_dead']})",
            f"  not terminal           {s['not_terminal']}",
            f"  executions             {s['executions']}  (reached effect: {s['reached_effect']})",
            f"  total kills            {s['kills']}  "
            f"(by phase seen: {s['kills_by_phase']}; misses: {s['misses']})",
            f"  worker restarts        {s['restarts']}",
            f"  leases reaped          {s['reaps']}",
            f"  duplicate executions   {s['duplicate_executions']}  "
            f"(jobs whose effect phase ran more than once: {s['jobs_with_duplicate_executions']})",
            f"  duplicate effects      {s['duplicate_effects']}  "
            f"(jobs with more than one effect row: {s['jobs_with_duplicate_effects']})",
            f"  wall-clock             {self.wall_seconds:.1f}s",
            f"  worker logs            {self.log_dir}",
        ]
        return "\n".join(lines)

    def report(self) -> str:
        """Summary plus, for each violation, the exact interleaving from the ledger."""
        out = [self.summary(), ""]
        if self.ok:
            out.append("OK: all assertions held")
            return "\n".join(out)
        out.append(f"FAILED: {len(self.violations)} violation(s)")
        out.append(
            "Times are seconds since run start. Rows carry DB clock_timestamp(); "
            "kills carry the host clock (same kernel under Compose)."
        )
        for v in self.violations:
            out.append("")
            out.append(f"* [{v.kind}] {v.message}")
            for line in self.timelines.get(v.job_id or -1, []):
                out.append(f"    {line}")
        return "\n".join(out)

    def to_dict(self) -> dict[str, Any]:
        return {
            "config": asdict(self.config),
            "run_id": self.run_id,
            "seed": self.seed,
            "ok": self.ok,
            "wall_seconds": self.wall_seconds,
            "stats": self.stats,
            "violations": [asdict(v) for v in self.violations],
            "timelines": {str(k): v for k, v in self.timelines.items()},
            "log_dir": self.log_dir,
        }


class _WorkerSlot:
    """One logical worker: a chain of process incarnations, restarted on death."""

    def __init__(self, harness: Harness, index: int) -> None:
        self.harness = harness
        self.index = index
        self.incarnation = 0
        self.proc: subprocess.Popen[str] | None = None
        self.worker_id = ""
        self.job_id: int | None = None
        self.attempt: int | None = None
        self.phase = "idle"
        self.lock = threading.Lock()

    def spawn(self) -> None:
        h = self.harness
        self.incarnation += 1
        worker_id = f"w{self.index}.{self.incarnation}"
        cfg = h.config
        cmd = [
            sys.executable,
            "-m",
            "conveyor.worker",
            "--handler",
            f"chaos.handlers:{cfg.mode}",
            "--worker-id",
            worker_id,
            "--visibility-timeout",
            str(cfg.visibility_timeout),
            "--poll-interval",
            "0.05",
            "--backoff-base",
            "0.1",
            "--backoff-cap",
            "1.0",
            "--heartbeat-interval",
            str(cfg.visibility_timeout / 3 if cfg.heartbeat else 0),
        ]
        stderr = open(Path(h.log_dir) / f"{worker_id}.log", "w")  # noqa: SIM115
        proc = subprocess.Popen(
            cmd,
            cwd=ROOT,
            env={**os.environ, "DATABASE_URL": h.dsn, "PYTHONUNBUFFERED": "1"},
            stdout=subprocess.PIPE,
            stderr=stderr,
            text=True,
        )
        stderr.close()
        with self.lock:
            self.proc = proc
            self.worker_id = worker_id
            self.job_id = self.attempt = None
            self.phase = "idle"
        if self.incarnation > 1:
            h.restarts += 1
        threading.Thread(target=self._pump, args=(proc, worker_id), daemon=True).start()

    def _pump(self, proc: subprocess.Popen[str], worker_id: str) -> None:
        h = self.harness
        assert proc.stdout is not None
        for line in proc.stdout:
            if not line.startswith("CHAOS "):
                continue
            fields = dict(kv.split("=", 1) for kv in line.split()[1:])
            job_id, attempt, phase = int(fields["job"]), int(fields["attempt"]), fields["phase"]
            with self.lock:
                self.job_id, self.attempt, self.phase = job_id, attempt, phase
            h.on_phase(self, worker_id, proc.pid, job_id, attempt, phase)
        rc = proc.wait()
        h.on_exit(self, worker_id, proc.pid, rc)
        if not h.stopping.is_set():
            time.sleep(h.config.restart_delay)
            if not h.stopping.is_set():
                self.spawn()

    def kill_if_still_on(self, pid: int, job_id: int, attempt: int) -> tuple[bool, str]:
        """SIGKILL the current process iff it is still executing (job_id, attempt)."""
        with self.lock:
            proc = self.proc
            if proc is None or proc.pid != pid or proc.poll() is not None:
                return False, "process already gone"
            if (self.job_id, self.attempt) != (job_id, attempt):
                return False, f"worker moved on to job={self.job_id} attempt={self.attempt}"
            phase = self.phase
            self.harness.killed_pids.add(pid)  # before the signal, so on_exit sees it
            proc.send_signal(signal.SIGKILL)
            return True, phase

    def terminate(self, grace: float) -> None:
        with self.lock:
            proc = self.proc
        if proc is None or proc.poll() is not None:
            return
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            self.harness.killed_pids.add(proc.pid)
            proc.kill()
            proc.wait()


class Harness:
    def __init__(self, config: ChaosConfig) -> None:
        config.validate()
        self.config = config
        self.dsn = config.dsn or dsn_from_env()
        self.seed = (
            config.seed if config.seed is not None else random.SystemRandom().getrandbits(32)
        )
        self.run_id = f"{config.mode}-{self.seed}-{dt.datetime.now(dt.UTC):%Y%m%dT%H%M%S}"
        self.log_dir = config.log_dir or tempfile.mkdtemp(prefix=f"conveyor-chaos-{self.run_id}-")
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)
        self.payloads: dict[int, dict[str, Any]] = {}
        self.slots: list[_WorkerSlot] = []
        self.stopping = threading.Event()
        self.lock = threading.Lock()
        self.kills: list[Kill] = []
        self.misses: list[Miss] = []
        self.reaps: list[Reap] = []
        self.exits: list[WorkerExit] = []
        self.killed_pids: set[int] = set()
        self.restarts = 0
        self.timers: list[threading.Timer] = []
        self.pending_after_effect: dict[tuple[int, int], tuple[_WorkerSlot, int, float]] = {}
        self.started_at = 0.0

    # ---- setup -----------------------------------------------------------------

    def _plan_jobs(self) -> list[dict[str, Any]]:
        rng = random.Random(self.seed)
        cfg = self.config
        plans = []
        for i in range(cfg.jobs):
            pre = round(rng.uniform(0, cfg.max_sleep), 3)
            post = round(rng.uniform(0, cfg.max_sleep), 3)
            fail = rng.random() < cfg.fail_probability
            overrun = (not fail) and rng.random() < cfg.overrun_probability
            plans.append(
                {
                    "run_id": self.run_id,
                    "i": i,
                    "pre": pre,
                    "post": post,
                    "fail": fail,
                    "overrun": overrun,  # attempt 1 sleeps overrun_post instead of post
                    "overrun_post": round(cfg.visibility_timeout + 0.5, 3),
                }
            )
        return plans

    def _enqueue_all(self, conn: psycopg.Connection[DictRow]) -> None:
        busy = conn.execute(
            "SELECT count(*) AS n FROM jobs WHERE status IN ('queued', 'running')"
        ).fetchone()["n"]
        conn.commit()
        if busy:
            raise RuntimeError(
                f"{busy} queued/running job(s) already in the queue; the chaos run needs a "
                "queue it owns (its workers would execute them with the chaos handler)"
            )
        for plan in self._plan_jobs():
            job, created = enqueue(
                conn,
                plan,
                idempotency_key=f"{self.run_id}:{plan['i']}",
                max_attempts=self.config.max_attempts,
            )
            assert created
            self.payloads[job.id] = plan

    # ---- events from worker stdout --------------------------------------------

    def on_phase(
        self, slot: _WorkerSlot, worker_id: str, pid: int, job_id: int, attempt: int, phase: str
    ) -> None:
        if self.stopping.is_set() or job_id not in self.payloads:
            return
        if phase == "started":
            self._decide_kill(slot, worker_id, pid, job_id, attempt)
        elif phase == "effect_done":
            with self.lock:
                pending = self.pending_after_effect.pop((job_id, attempt), None)
            if pending is not None:
                slot_, pid_, delay = pending
                self._schedule(delay, slot_, pid_, job_id, attempt, "after_effect")

    def _decide_kill(
        self, slot: _WorkerSlot, worker_id: str, pid: int, job_id: int, attempt: int
    ) -> None:
        cfg = self.config
        if attempt > cfg.max_attempts - 2:
            return  # targeting rule: the last two attempts are never killed
        rng = random.Random(f"{self.seed}:{job_id}:{attempt}")
        if rng.random() >= cfg.kill_probability:
            return
        plan = dict(self.payloads[job_id])
        if plan["overrun"] and attempt == 1:
            plan["post"] = plan["overrun_post"]
        timing = cfg.kill_timing
        if timing == "mixed":
            timing = "before_effect" if rng.random() < 0.5 else "after_effect"
        if timing == "before_effect":
            self._schedule(rng.uniform(0, plan["pre"]), slot, pid, job_id, attempt, timing)
        elif timing == "after_effect":
            delay = rng.uniform(0, plan["post"])
            with self.lock:
                self.pending_after_effect[(job_id, attempt)] = (slot, pid, delay)
        else:  # random: anywhere in the handler's lifetime
            total = plan["pre"] + plan["post"]
            self._schedule(rng.uniform(0, total), slot, pid, job_id, attempt, timing)

    def _schedule(
        self, delay: float, slot: _WorkerSlot, pid: int, job_id: int, attempt: int, timing: str
    ) -> None:
        timer = threading.Timer(delay, self._fire, args=(slot, pid, job_id, attempt, timing))
        timer.daemon = True
        with self.lock:
            self.timers.append(timer)
        timer.start()

    def _fire(self, slot: _WorkerSlot, pid: int, job_id: int, attempt: int, timing: str) -> None:
        if self.stopping.is_set():
            return
        killed, detail = slot.kill_if_still_on(pid, job_id, attempt)
        now = time.time()
        with self.lock:
            if killed:
                self.kills.append(Kill(now, slot.worker_id, pid, job_id, attempt, timing, detail))
            else:
                self.misses.append(Miss(now, slot.worker_id, job_id, attempt, timing, detail))
        if killed:
            log.info(
                "SIGKILL %s pid=%d job=%d attempt=%d (%s, phase seen: %s)",
                slot.worker_id,
                pid,
                job_id,
                attempt,
                timing,
                detail,
            )

    def on_exit(self, slot: _WorkerSlot, worker_id: str, pid: int, rc: int) -> None:
        with self.lock:
            ours = pid in self.killed_pids
            self.exits.append(WorkerExit(time.time(), worker_id, pid, rc, ours))

    # ---- reaper ------------------------------------------------------------------

    def _reaper_loop(self) -> None:
        with connect(self.dsn) as conn:
            while not self.stopping.is_set():
                for row in reap_expired(conn):
                    with self.lock:
                        self.reaps.append(
                            Reap(
                                row["reaped_at"].timestamp(),
                                row["id"],
                                row["attempts"],
                                row["status"],
                            )
                        )
                    log.info(
                        "reaped job=%d attempt=%d -> %s", row["id"], row["attempts"], row["status"]
                    )
                self.stopping.wait(self.config.reaper_interval)

    # ---- run ---------------------------------------------------------------------

    def run(self) -> RunResult:
        cfg = self.config
        ids: list[int]
        with connect(self.dsn) as conn:
            conn.execute(SCHEMA.read_text())
            conn.commit()
            self._enqueue_all(conn)
            ids = sorted(self.payloads)
            log.info(
                "run %s: enqueued %d jobs, starting %d workers", self.run_id, len(ids), cfg.workers
            )

            self.started_at = time.time()
            reaper = threading.Thread(target=self._reaper_loop, daemon=True)
            reaper.start()
            for i in range(cfg.workers):
                slot = _WorkerSlot(self, i)
                self.slots.append(slot)
                slot.spawn()

            deadline = self.started_at + cfg.duration
            timed_out = False
            while True:
                counts = self._status_counts(conn, ids)
                done = counts.get("succeeded", 0) + counts.get("dead", 0)
                if done == len(ids):
                    break
                if time.time() > deadline:
                    timed_out = True
                    break
                time.sleep(0.5)

            self._stop_everything(reaper)
            wall = time.time() - self.started_at
            return self._verify(conn, ids, wall, timed_out)

    def _status_counts(self, conn: psycopg.Connection[DictRow], ids: list[int]) -> dict[str, int]:
        rows = conn.execute(
            "SELECT status, count(*) AS n FROM jobs WHERE id = ANY(%s) GROUP BY status", (ids,)
        ).fetchall()
        conn.commit()
        return {r["status"]: r["n"] for r in rows}

    def _stop_everything(self, reaper: threading.Thread) -> None:
        self.stopping.set()
        with self.lock:
            timers = list(self.timers)
        for t in timers:
            t.cancel()
        for slot in self.slots:
            slot.terminate(grace=10.0)
        reaper.join(timeout=10)

    # ---- verification ------------------------------------------------------------

    def _verify(
        self, conn: psycopg.Connection[DictRow], ids: list[int], wall: float, timed_out: bool
    ) -> RunResult:
        cfg = self.config
        jobs = {
            r["id"]: r
            for r in conn.execute(
                "SELECT id, status, attempts, max_attempts, last_error, claimed_by, "
                "claimed_at, finished_at FROM jobs WHERE id = ANY(%s)",
                (ids,),
            ).fetchall()
        }
        effects: dict[int, list[DictRow]] = {i: [] for i in ids}
        for r in conn.execute(
            "SELECT job_id, attempt, worker, applied_at FROM chaos_effects "
            "WHERE run_id = %s ORDER BY applied_at",
            (self.run_id,),
        ).fetchall():
            effects[r["job_id"]].append(r)
        executions: dict[int, list[DictRow]] = {i: [] for i in ids}
        for r in conn.execute(
            "SELECT job_id, attempt, worker, pid, started_at, effect_at, effect_inserted, "
            "finished_at FROM chaos_executions WHERE run_id = %s ORDER BY started_at",
            (self.run_id,),
        ).fetchall():
            executions[r["job_id"]].append(r)
        conn.commit()

        violations: list[Violation] = []
        counts: dict[str, int] = {}
        expected_dead = 0
        duplicate_executions = 0
        duplicate_effects = 0
        jobs_dup_exec = 0
        jobs_dup_eff = 0
        reached_effect = 0

        for job_id in ids:
            j = jobs[job_id]
            plan = self.payloads[job_id]
            counts[j["status"]] = counts.get(j["status"], 0) + 1
            n_eff = len(effects[job_id])
            n_reached = sum(1 for e in executions[job_id] if e["effect_at"] is not None)
            reached_effect += n_reached
            if n_reached > 1:
                duplicate_executions += n_reached - 1
                jobs_dup_exec += 1
            if n_eff > 1:
                duplicate_effects += n_eff - 1
                jobs_dup_eff += 1

            def bad(kind: str, msg: str, _id: int = job_id) -> None:
                violations.append(Violation(kind, f"job {_id}: {msg}", _id))

            if j["status"] not in TERMINAL:
                bad(
                    "not_terminal",
                    f"status={j['status']} attempts={j['attempts']}"
                    + (" (run duration exceeded)" if timed_out else ""),
                )
                continue
            if plan["fail"]:
                expected_dead += 1
                if j["status"] != "dead":
                    bad("expected_dead_wrong", f"configured to fail but status={j['status']}")
                elif n_eff:
                    bad("expected_dead_wrong", f"configured to fail but has {n_eff} effect(s)")
                elif j["attempts"] != j["max_attempts"]:
                    bad(
                        "expected_dead_wrong",
                        f"dead after {j['attempts']} of {j['max_attempts']} attempts",
                    )
                continue
            if j["status"] == "dead":
                bad(
                    "unexpected_dead",
                    f"dead after {j['attempts']} attempts, last_error={j['last_error']!r}",
                )
            if n_eff == 0:
                bad("lost", f"status={j['status']} but zero effect rows")
            elif cfg.mode == "idempotent" and n_eff > 1:
                bad("duplicate", f"idempotent handler left {n_eff} effect rows")

        chaos_expected = cfg.kill_probability > 0 or cfg.overrun_probability > 0
        if chaos_expected:
            if not self.kills and cfg.kill_probability > 0:
                violations.append(
                    Violation("window_not_exercised", "kill_probability > 0 but no kill landed")
                )
            if cfg.mode == "naive" and duplicate_effects == 0:
                violations.append(
                    Violation(
                        "window_not_exercised",
                        "naive handler shows zero duplicate effects: the failure window was never "
                        "hit, so an idempotent pass would prove nothing",
                    )
                )
            if cfg.mode == "idempotent" and duplicate_executions == 0:
                violations.append(
                    Violation(
                        "window_not_exercised",
                        "no job reached the effect phase twice: the failure window was never hit",
                    )
                )
        for e in self.exits:
            if e.returncode != 0 and not e.killed_by_us:
                violations.append(
                    Violation(
                        "unexpected_worker_exit",
                        f"{e.worker} pid={e.pid} exited with {e.returncode} without being killed "
                        f"by the harness; see {Path(self.log_dir) / (e.worker + '.log')}",
                    )
                )

        kills_by_phase: dict[str, int] = {}
        for k in self.kills:
            kills_by_phase[k.phase_seen] = kills_by_phase.get(k.phase_seen, 0) + 1
        stats = {
            "enqueued": len(ids),
            "succeeded": counts.get("succeeded", 0),
            "dead": counts.get("dead", 0),
            "expected_dead": expected_dead,
            "not_terminal": len(ids) - counts.get("succeeded", 0) - counts.get("dead", 0),
            "executions": sum(len(v) for v in executions.values()),
            "reached_effect": reached_effect,
            "kills": len(self.kills),
            "kills_by_phase": kills_by_phase,
            "misses": len(self.misses),
            "restarts": self.restarts,
            "reaps": len(self.reaps),
            "duplicate_executions": duplicate_executions,
            "jobs_with_duplicate_executions": jobs_dup_exec,
            "duplicate_effects": duplicate_effects,
            "jobs_with_duplicate_effects": jobs_dup_eff,
        }
        timelines = {
            v.job_id: self._timeline(
                v.job_id, jobs[v.job_id], executions[v.job_id], effects[v.job_id]
            )
            for v in violations
            if v.job_id is not None
        }
        return RunResult(
            config=cfg,
            run_id=self.run_id,
            seed=self.seed,
            started_at=self.started_at,
            wall_seconds=wall,
            stats=stats,
            violations=violations,
            timelines=timelines,
            log_dir=self.log_dir,
        )

    def _timeline(
        self, job_id: int, job: DictRow, execs: list[DictRow], effs: list[DictRow]
    ) -> list[str]:
        """Merge every recorded event for one job into a time-ordered list of lines."""
        t0 = self.started_at

        def rel(ts: dt.datetime | float | None) -> float:
            if ts is None:
                return float("inf")
            return (ts.timestamp() if isinstance(ts, dt.datetime) else ts) - t0

        events: list[tuple[float, str]] = []
        for e in execs:
            who = f"{e['worker']} pid={e['pid']}"
            events.append((rel(e["started_at"]), f"attempt {e['attempt']} started by {who}"))
            if e["effect_at"] is not None:
                what = "inserted" if e["effect_inserted"] else "found key, inserted nothing"
                events.append(
                    (rel(e["effect_at"]), f"attempt {e['attempt']} committed effect phase ({what})")
                )
            if e["finished_at"] is not None:
                events.append(
                    (
                        rel(e["finished_at"]),
                        f"attempt {e['attempt']} handler finished (ack follows)",
                    )
                )
        for f in effs:
            events.append(
                (
                    rel(f["applied_at"]),
                    f"EFFECT row written by attempt {f['attempt']} ({f['worker']})",
                )
            )
        for k in self.kills:
            if k.job_id == job_id:
                events.append(
                    (
                        rel(k.t),
                        f"SIGKILL {k.worker} pid={k.pid} during attempt {k.attempt} "
                        f"(timing={k.timing}, phase seen={k.phase_seen})",
                    )
                )
        for m in self.misses:
            if m.job_id == job_id:
                events.append((rel(m.t), f"kill missed for attempt {m.attempt}: {m.reason}"))
        for r in self.reaps:
            if r.job_id == job_id:
                events.append((rel(r.t), f"REAPED attempt {r.attempt} -> {r.status}"))
        if job["finished_at"] is not None:
            events.append(
                (
                    rel(job["finished_at"]),
                    f"job {job['status']} at attempt {job['attempts']} "
                    f"(last_error={job['last_error']!r})",
                )
            )
        events.sort(key=lambda ev: ev[0])
        lines = [f"T+{t:8.3f}s  {text}" for t, text in events]
        lines.append(
            f"final row: status={job['status']} attempts={job['attempts']}/{job['max_attempts']} "
            f"claimed_by={job['claimed_by']} effects={len(effs)} "
            f"executions={len(execs)} plan={json.dumps(self.payloads[job_id])}"
        )
        return lines


def run(config: ChaosConfig) -> RunResult:
    return Harness(config).run()
