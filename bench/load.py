"""Load test: throughput, scaling, latency, and where the ceiling actually is.

    python -m bench.load all --out bench/reports
    python -m bench.load sweep --workers 1,2,4,8,16 --reps 3
    python -m bench.load probe --workers 16
    python -m bench.load heartbeat
    python -m bench.load steady --workers 8 --arrival-rate 800

Nothing here tunes anything. Postgres runs the stock Compose configuration, no
indexes are added, claims are not batched, and there is no connection pooler.
The one deviation is ``--sync-commit off``, a session-level setting used only by
the labeled diagnostic in ``all``; it never contributes a headline number.

Measurement decisions worth knowing before reading any number this produces:

* **Throughput is measured over the interior of each run.** Completion times
  come from the database (``jobs.finished_at``), are sorted, and the rate is
  taken between the 10th and 90th percentile completion. That drops worker
  ramp-up and the tail where workers idle against an empty queue. Wall-clock
  throughput is reported next to it so the gap is visible.
* **Two clocks, each used for one job.** Database timestamps give throughput and
  end-to-end latency, so there is no host/DB skew in them. ``perf_counter`` in
  the worker gives claim and ack latency, which are round-trips the database
  cannot see the start of.
* **Job count scales with worker count** so every point runs for a comparable
  time. Throughput is a rate, so this does not bias it; the count is reported.
* Each run TRUNCATEs the jobs table. The bench owns the database it points at.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import DictRow

from bench import env as bench_env
from conveyor.db import connect, dsn_from_env
from conveyor.queue import enqueue

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_WORKER_COUNTS = (1, 2, 4, 8, 16)
# Postgres backends flush pending stats at most once a second, so counter reads
# taken either side of a run need a pause to avoid attributing setup work to it.
STATS_SETTLE = 1.2


# --------------------------------------------------------------------------------------
# configuration
# --------------------------------------------------------------------------------------


@dataclass
class BenchConfig:
    workers: int = 8
    jobs_per_worker: int = 900
    min_jobs: int = 2000
    handler_sleep: float = 0.0
    heartbeat_interval: float = 0.0  # 0 disables; >0 fires every N seconds
    visibility_timeout: float = 30.0
    sync_commit: str = "on"
    poll_interval: float = 0.02
    timeout: float = 300.0
    real_worker: bool = False  # use the product worker instead of bench.worker
    label: str = ""
    dsn: str | None = None

    def job_count(self) -> int:
        return max(self.min_jobs, self.jobs_per_worker * self.workers)


# --------------------------------------------------------------------------------------
# samplers
# --------------------------------------------------------------------------------------


class WaitSampler:
    """Poll pg_stat_activity for wait events while a run is in flight.

    This is how the bottleneck gets attributed instead of guessed.
    ``Lock/transactionid`` and ``Lock/tuple`` mean workers are contending for the
    same rows; ``IO/WALSync`` and ``LWLock/WAL*`` mean commit durability;
    ``Client/ClientRead`` means the backend is idle waiting on the application,
    so Postgres is not the constraint; ``active`` with no wait event is CPU
    inside Postgres.

    Needs no server configuration change (``track_activities`` is on by
    default), which is why this is used instead of ``pg_stat_statements``.
    """

    def __init__(self, dsn: str, interval: float = 0.005) -> None:
        self.dsn = dsn
        self.interval = interval
        self.counts: Counter[str] = Counter()
        self.samples = 0
        self.backend_states: Counter[str] = Counter()
        self.lock_waiters = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def _loop(self) -> None:
        with connect(self.dsn) as conn:
            conn.execute("SET application_name = 'bench-sampler'")
            conn.commit()
            while not self._stop.is_set():
                try:
                    rows = conn.execute(
                        """
                        SELECT state, wait_event_type, wait_event
                        FROM pg_stat_activity
                        WHERE datname = current_database()
                          AND pid <> pg_backend_pid()
                          AND coalesce(application_name, '') <> 'bench-sampler'
                        """
                    ).fetchall()
                    waiters = conn.execute(
                        "SELECT count(*) AS n FROM pg_locks WHERE NOT granted"
                    ).fetchone()["n"]
                    conn.commit()
                except psycopg.Error:
                    conn.rollback()
                    continue
                self.samples += 1
                self.lock_waiters += waiters
                for r in rows:
                    self.backend_states[r["state"] or "unknown"] += 1
                    if r["state"] != "active":
                        continue
                    if r["wait_event_type"] is None:
                        self.counts["CPU (running, no wait)"] += 1
                    else:
                        self.counts[f"{r['wait_event_type']}/{r['wait_event']}"] += 1
                self._stop.wait(self.interval)

    def __enter__(self) -> WaitSampler:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    def result(self) -> dict[str, Any]:
        total = sum(self.counts.values())
        return {
            "samples": self.samples,
            "active_backend_observations": total,
            "wait_events": [
                {"event": k, "observations": v, "share": round(v / total, 4) if total else 0.0}
                for k, v in self.counts.most_common(12)
            ],
            "backend_states": dict(self.backend_states),
            "ungranted_lock_observations": self.lock_waiters,
        }


class CpuSampler:
    """Host CPU busy fraction from /proc/stat deltas.

    Whole-host, so it covers the workers and Postgres together: they share this
    machine. Used only to answer "is anything CPU-saturated", which is enough to
    rule CPU in or out. No new dependency (psutil is not installed).
    """

    def __init__(self) -> None:
        self.cores = max(1, len(_proc_stat_cpu_lines()) - 1)
        self.start = self._read()

    @staticmethod
    def _read() -> tuple[int, int]:
        lines = _proc_stat_cpu_lines()
        if not lines:
            return (0, 0)
        fields = [int(x) for x in lines[0].split()[1:]]
        idle = fields[3] + (fields[4] if len(fields) > 4 else 0)
        return (sum(fields), idle)

    def finish(self) -> dict[str, Any]:
        end = self._read()
        total = end[0] - self.start[0]
        idle = end[1] - self.start[1]
        if total <= 0:
            return {"host_cpu_busy_fraction": None, "cores": self.cores}
        busy = 1 - idle / total
        return {
            "host_cpu_busy_fraction": round(busy, 4),
            "host_cpu_busy_cores_equivalent": round(busy * self.cores, 2),
            "cores": self.cores,
        }


def _proc_stat_cpu_lines() -> list[str]:
    try:
        return [ln for ln in Path("/proc/stat").read_text().splitlines() if ln.startswith("cpu")]
    except OSError:
        return []


# --------------------------------------------------------------------------------------
# statistics helpers
# --------------------------------------------------------------------------------------


def percentiles(values: list[float], ps: tuple[float, ...] = (50, 95, 99)) -> dict[str, float]:
    """Nearest-rank percentiles. Spelled out so the definition is not a library detail."""
    if not values:
        return {f"p{p:g}": float("nan") for p in ps}
    ordered = sorted(values)
    out = {}
    for p in ps:
        rank = max(1, math.ceil(p / 100 * len(ordered)))
        out[f"p{p:g}"] = ordered[min(rank, len(ordered)) - 1]
    return out


def ms(seconds: float) -> float:
    return round(seconds * 1000, 3)


def _interior_rate(finish_times: list[float]) -> tuple[float, float, int]:
    """Rate over the middle 80% of completions, excluding ramp-up and drain."""
    if len(finish_times) < 20:
        if len(finish_times) < 2:
            return 0.0, 0.0, len(finish_times)
        span = finish_times[-1] - finish_times[0]
        return (len(finish_times) / span if span > 0 else 0.0), span, len(finish_times)
    lo = int(len(finish_times) * 0.10)
    hi = int(len(finish_times) * 0.90)
    span = finish_times[hi - 1] - finish_times[lo]
    count = hi - lo
    return (count / span if span > 0 else 0.0), span, count


# --------------------------------------------------------------------------------------
# one run
# --------------------------------------------------------------------------------------


@dataclass
class RunResult:
    config: dict[str, Any]
    jobs: int
    completed: int
    wall_seconds: float
    interior_throughput: float
    wallclock_throughput: float
    interior_window_seconds: float
    interior_jobs: int
    achieved_arrival_rate: float | None
    claim_latency_ms: dict[str, float]
    ack_latency_ms: dict[str, float]
    e2e_latency_ms: dict[str, float]
    worker_stats: dict[str, Any]
    waits: dict[str, Any]
    cpu: dict[str, Any]
    db: dict[str, Any]
    notes: list[str] = field(default_factory=list)


def _guard_and_reset(conn: psycopg.Connection[DictRow]) -> None:
    busy = conn.execute(
        "SELECT count(*) AS n FROM jobs WHERE status IN ('queued', 'running')"
    ).fetchone()["n"]
    conn.commit()
    if busy:
        raise RuntimeError(
            f"{busy} queued/running job(s) already present. The bench truncates and owns the "
            "jobs table; point DATABASE_URL at a database you are willing to clear."
        )
    _reset(conn)


def _reset(conn: psycopg.Connection[DictRow]) -> None:
    """Clear the queue without the guard, for jobs this process just created."""
    conn.execute("TRUNCATE jobs RESTART IDENTITY")
    conn.commit()


def _preload(conn: psycopg.Connection[DictRow], n: int, label: str) -> float:
    """Fill the queue for a batch-drain run. Setup only, never a measured path.

    This uses COPY rather than ``conveyor.queue.enqueue`` because the setup for
    a 40,000-job run must not take longer than the run. It writes the same
    columns ``enqueue`` writes and leaves every other column at its schema
    default, so the workers see exactly the rows they would otherwise see. The
    real enqueue path is measured on its own by ``measure_enqueue``, and that
    number is what the report quotes for enqueue throughput.
    """
    t0 = time.monotonic()
    with (
        conn.transaction(),
        conn.cursor().copy("COPY jobs (idempotency_key, payload) FROM STDIN") as cp,
    ):
        for i in range(n):
            cp.write_row((f"{label}:{i}", json.dumps({"bench": label, "i": i})))
    return time.monotonic() - t0


def measure_enqueue(conn: psycopg.Connection[DictRow], n: int, label: str) -> dict[str, Any]:
    """Throughput of the real ``enqueue``, one commit per job, single connection.

    Reported separately from job throughput because it is a different operation
    with a different cost: one commit instead of the claim/ack pair.
    """
    latencies: list[float] = []
    t0 = time.monotonic()
    for i in range(n):
        t = time.perf_counter()
        enqueue(conn, {"bench": label, "i": i}, idempotency_key=f"{label}:{i}")
        latencies.append(time.perf_counter() - t)
    elapsed = time.monotonic() - t0
    return {
        "jobs": n,
        "seconds": round(elapsed, 3),
        "enqueues_per_second": round(n / elapsed, 1),
        "latency_ms": {k: ms(v) for k, v in percentiles(latencies).items()},
        "connections": 1,
    }


def _db_counters(conn: psycopg.Connection[DictRow]) -> dict[str, int]:
    row = conn.execute(
        """
        SELECT xact_commit, xact_rollback, tup_fetched, tup_updated, tup_inserted,
               blks_hit, blks_read, deadlocks
        FROM pg_stat_database WHERE datname = current_database()
        """
    ).fetchone()
    conn.commit()
    return dict(row)


def _spawn_workers(cfg: BenchConfig, dsn: str, sample_dir: Path) -> list[subprocess.Popen[str]]:
    procs = []
    child_env = {"DATABASE_URL": dsn, "PYTHONUNBUFFERED": "1", "PATH": os.environ.get("PATH", "")}
    for i in range(cfg.workers):
        if cfg.real_worker:
            # The unmodified product worker, for the fidelity cross-check.
            cmd = [
                sys.executable,
                "-m",
                "conveyor.worker",
                "--handler",
                "bench.handlers:noop",
                "--worker-id",
                f"real{i}",
                "--visibility-timeout",
                str(cfg.visibility_timeout),
                "--poll-interval",
                str(cfg.poll_interval),
                "--heartbeat-interval",
                str(cfg.heartbeat_interval),
            ]
        else:
            cmd = [
                sys.executable,
                "-m",
                "bench.worker",
                "--worker-id",
                f"w{i}",
                "--out",
                str(sample_dir / f"w{i}.json"),
                "--visibility-timeout",
                str(cfg.visibility_timeout),
                "--handler-sleep",
                str(cfg.handler_sleep),
                "--heartbeat-interval",
                str(cfg.heartbeat_interval),
                "--poll-interval",
                str(cfg.poll_interval),
                "--sync-commit",
                cfg.sync_commit,
            ]
        procs.append(
            subprocess.Popen(
                cmd,
                cwd=ROOT,
                env=child_env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
        )
    return procs


def _stop_workers(procs: list[subprocess.Popen[str]]) -> None:
    """SIGTERM, which both workers treat as 'finish the current job and exit'."""
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(timeout=20)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()


def _read_samples(sample_dir: Path) -> tuple[list[float], list[float], dict[str, int]]:
    claim: list[float] = []
    ackl: list[float] = []
    stats = {"processed": 0, "empty_claims": 0, "extends": 0, "lease_lost": 0, "ack_rejected": 0}
    for path in sorted(sample_dir.glob("*.json")):
        try:
            data = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        claim.extend(data.get("claim_latencies", []))
        ackl.extend(data.get("ack_latencies", []))
        for k in stats:
            stats[k] += data.get(k, 0)
    return claim, ackl, stats


def run_once(cfg: BenchConfig, *, arrival_rate: float | None = None) -> RunResult:
    """One measurement.

    ``arrival_rate`` None means batch drain: enqueue everything up front, then
    let the workers eat it. A number means steady state: enqueue at that many
    jobs per second while the workers run, which is the only mode in which
    end-to-end latency measures responsiveness rather than backlog depth.
    """
    dsn = cfg.dsn or dsn_from_env()
    label = cfg.label or f"run-{datetime.now(UTC):%H%M%S%f}"
    n = cfg.job_count()
    notes: list[str] = []

    with connect(dsn) as conn, tempfile.TemporaryDirectory(prefix="conveyor-bench-") as tmp:
        sample_dir = Path(tmp)
        _guard_and_reset(conn)

        feeders: list[threading.Thread] = []
        feeder_stop = threading.Event()
        if arrival_rate is None:
            _preload(conn, n, label)
        else:
            feeders = _feeders(dsn, n, label, arrival_rate, feeder_stop)

        # Backends flush their pending stats to pg_stat_database at most once a
        # second, so reading the counters immediately after the preload
        # attributes the preload's commits to the run. Settle first.
        time.sleep(STATS_SETTLE)
        before = _db_counters(conn)
        cpu = CpuSampler()
        t_start = time.monotonic()
        with WaitSampler(dsn) as waits:
            procs = _spawn_workers(cfg, dsn, sample_dir)
            for f in feeders:
                f.start()
            deadline = t_start + cfg.timeout
            while True:
                completed = conn.execute(
                    "SELECT count(*) AS n FROM jobs WHERE status = 'succeeded'"
                ).fetchone()["n"]
                conn.commit()
                if completed >= n:
                    break
                if time.monotonic() > deadline:
                    notes.append(
                        f"TIMEOUT after {cfg.timeout}s with {completed}/{n} completed. "
                        "Rates below are computed from what actually finished."
                    )
                    break
                time.sleep(0.05)
            wall = time.monotonic() - t_start
            feeder_stop.set()
            for f in feeders:
                f.join(timeout=10)
            _stop_workers(procs)
        cpu_result = cpu.finish()
        waits_result = waits.result()
        time.sleep(STATS_SETTLE)  # let the workers' final stats reach the collector
        after = _db_counters(conn)

        # Throughput and end-to-end latency come from database timestamps: one
        # clock, so no host/DB skew enters these numbers.
        rows = conn.execute(
            """
            SELECT extract(epoch FROM finished_at) AS fin,
                   extract(epoch FROM created_at) AS created,
                   extract(epoch FROM finished_at - created_at) AS e2e
            FROM jobs WHERE status = 'succeeded' ORDER BY finished_at
            """
        ).fetchall()
        conn.commit()
        claim_lat, ack_lat, wstats = _read_samples(sample_dir)

    fins = [float(r["fin"]) for r in rows]
    e2e = [float(r["e2e"]) for r in rows]
    interior_rate, window, interior_n = _interior_rate(fins)
    db_delta = {k: after[k] - before[k] for k in before}
    if fins:
        db_delta["commits_per_job"] = round(db_delta["xact_commit"] / len(fins), 2)
    if cfg.real_worker:
        notes.append("Ran the product worker (conveyor.worker); per-call latencies unavailable.")

    achieved_arrival = None
    if arrival_rate is not None:
        created = sorted(float(r["created"]) for r in rows)
        span = created[-1] - created[0] if len(created) > 1 else 0.0
        achieved_arrival = round(len(created) / span, 1) if span > 0 else 0.0
        if achieved_arrival < arrival_rate * 0.95:
            notes.append(
                f"Arrival rate fell short: asked {arrival_rate:.0f}/s, achieved "
                f"{achieved_arrival:.0f}/s. The latency percentiles below describe the rate "
                "that actually happened, not the one requested."
            )

    return RunResult(
        config=asdict(cfg),
        jobs=n,
        completed=len(fins),
        wall_seconds=round(wall, 3),
        interior_throughput=round(interior_rate, 1),
        wallclock_throughput=round(len(fins) / wall if wall > 0 else 0.0, 1),
        interior_window_seconds=round(window, 3),
        interior_jobs=interior_n,
        achieved_arrival_rate=achieved_arrival,
        claim_latency_ms={k: ms(v) for k, v in percentiles(claim_lat).items()},
        ack_latency_ms={k: ms(v) for k, v in percentiles(ack_lat).items()},
        e2e_latency_ms={k: ms(v) for k, v in percentiles(e2e).items()},
        worker_stats=wstats,
        waits=waits_result,
        cpu=cpu_result,
        db=db_delta,
        notes=notes,
    )


FEED_PER_CONNECTION = 700.0  # safely under the measured single-connection enqueue rate


def _feed_shard(
    dsn: str, ids: range, label: str, rate: float, stop: threading.Event, start: float
) -> None:
    """Enqueue this shard's jobs on a schedule, one connection, product path."""
    with connect(dsn) as conn:
        interval = 1.0 / rate
        for k, i in enumerate(ids):
            if stop.is_set():
                return
            delay = start + k * interval - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            enqueue(conn, {"bench": label, "i": i}, idempotency_key=f"{label}:{i}")


def _feeders(
    dsn: str, n: int, label: str, rate: float, stop: threading.Event
) -> list[threading.Thread]:
    """Split a target arrival rate across enough connections to actually reach it.

    A single connection tops out near 1100 enqueues/s here (one commit per job),
    so driving 2000/s from one thread would silently produce a slower arrival
    rate than requested and a latency number that describes a different
    experiment. The achieved rate is measured afterwards from ``created_at`` and
    reported next to the target.
    """
    shards = max(1, math.ceil(rate / FEED_PER_CONNECTION))
    start = time.monotonic() + 0.2  # let every thread reach its loop first
    threads = []
    for s in range(shards):
        ids = range(s, n, shards)
        threads.append(
            threading.Thread(
                target=_feed_shard,
                args=(dsn, ids, label, rate / shards, stop, start),
                daemon=True,
            )
        )
    return threads


# --------------------------------------------------------------------------------------
# commit-ceiling probe
# --------------------------------------------------------------------------------------

_PROBE_CHILD = """
import sys, time
from conveyor.db import connect
seconds = float(sys.argv[1])
with connect() as conn:
    count, end = 0, time.monotonic() + seconds
    while time.monotonic() < end:
        conn.execute("INSERT INTO bench_probe DEFAULT VALUES")
        conn.commit()
        count += 1
print(count)
"""


def commit_probe(concurrency: int, seconds: float = 6.0, dsn: str | None = None) -> dict[str, Any]:
    """Max commits/sec at this concurrency with no queue logic involved.

    The queue spends two commits per job (claim, ack), so this number divided by
    two is the ceiling the queue could reach if commit durability were the only
    cost. Comparing the two says whether the claim query is the problem or the
    write path is.
    """
    dsn = dsn or dsn_from_env()
    child_env = {"DATABASE_URL": dsn, "PYTHONUNBUFFERED": "1", "PATH": os.environ.get("PATH", "")}
    # Created once here, not in the children: concurrent CREATE TABLE IF NOT
    # EXISTS races in the system catalogue and kills roughly half of them, which
    # silently measures a lower concurrency than requested.
    with connect(dsn) as conn:
        conn.execute("DROP TABLE IF EXISTS bench_probe")
        conn.execute("CREATE TABLE bench_probe (id bigserial primary key)")
        conn.commit()
    t0 = time.monotonic()
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", _PROBE_CHILD, str(seconds)],
            cwd=ROOT,
            env=child_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(concurrency)
    ]
    total = 0
    survivors = 0
    for p in procs:
        out, err = p.communicate(timeout=seconds + 60)
        if p.returncode != 0:
            raise RuntimeError(f"commit probe child failed: {err.strip()[:400]}")
        total += int(out.strip() or 0)
        survivors += 1
    if survivors != concurrency:
        raise RuntimeError(f"only {survivors}/{concurrency} probe children reported")
    elapsed = time.monotonic() - t0
    with connect(dsn) as conn:
        conn.execute("DROP TABLE IF EXISTS bench_probe")
        conn.commit()
    return {
        "concurrency": concurrency,
        "seconds": round(elapsed, 2),
        "commits": total,
        "commits_per_second": round(total / elapsed, 1),
        "implied_job_ceiling_per_second": round(total / elapsed / 2, 1),
    }


# --------------------------------------------------------------------------------------
# modes
# --------------------------------------------------------------------------------------


def calibrate(cfg: BenchConfig, target_seconds: float) -> int:
    """Pick a job count so this worker count runs for roughly ``target_seconds``.

    A short pilot measures the rate, then the real run is sized from it. Without
    this, a fixed job count makes one worker run for a minute while sixteen
    finish in two seconds, and a two-second run is mostly ramp-up. Throughput is
    a rate, so the job count does not bias it; it only decides how long we
    watch. The chosen count is reported with every point.
    """
    pilot = BenchConfig(
        **{**asdict(cfg), "jobs_per_worker": 0, "min_jobs": 1500, "label": f"{cfg.label}-pilot"}
    )
    rate = run_once(pilot).interior_throughput
    if rate <= 0:
        return 5000
    return int(max(2000, min(120_000, rate * target_seconds)))


def sweep(
    worker_counts: tuple[int, ...],
    reps: int,
    base: BenchConfig,
    dsn: str | None = None,
    target_seconds: float = 12.0,
) -> list[dict[str, Any]]:
    points = []
    for w in worker_counts:
        probe = BenchConfig(**{**asdict(base), "workers": w, "dsn": dsn, "label": f"cal-w{w}"})
        n = calibrate(probe, target_seconds)
        print(f"  workers={w:2d}: sized to {n} jobs for ~{target_seconds:.0f}s runs")
        runs = []
        for rep in range(reps):
            cfg = BenchConfig(
                **{
                    **asdict(base),
                    "workers": w,
                    "dsn": dsn,
                    "jobs_per_worker": 0,
                    "min_jobs": n,
                    "label": f"sweep-w{w}-r{rep}",
                }
            )
            print(f"    rep {rep + 1}/{reps} ... ", end="", flush=True)
            r = run_once(cfg)
            print(
                f"{r.interior_throughput:8.0f} jobs/s "
                f"({r.completed}/{r.jobs} in {r.wall_seconds:.1f}s)"
            )
            runs.append(asdict(r))
        rates = [r["interior_throughput"] for r in runs]
        points.append(
            {
                "workers": w,
                "jobs_per_run": runs[0]["jobs"],
                "reps": reps,
                "throughput_median": round(statistics.median(rates), 1),
                "throughput_min": min(rates),
                "throughput_max": max(rates),
                "runs": runs,
            }
        )
    return points


def heartbeat_cost(
    base: BenchConfig, dsn: str | None = None, *, workers: int = 16, reps: int = 3
) -> dict[str, Any]:
    """Two measurements, because heartbeating has two separable costs.

    1. Thread overhead alone: no-op handler with the product default interval
       (visibility timeout / 3 = 10s), so no extension ever fires. What is left
       is the cost of starting and joining a thread per job.
    2. Real extension cost: a 300 ms handler with a 100 ms interval, so roughly
       two extend_lease round-trips per job, against the same handler with
       heartbeating off.
    """
    out: dict[str, Any] = {}
    thread_only = {}
    for name, hb in (("heartbeat_off", 0.0), ("heartbeat_on_never_fires", 10.0)):
        runs = []
        for rep in range(reps):
            cfg = BenchConfig(
                **{
                    **asdict(base),
                    "workers": workers,
                    "jobs_per_worker": 0,
                    "min_jobs": 30_000,
                    "heartbeat_interval": hb,
                    "dsn": dsn,
                    "label": f"hb-thread-{name}-{rep}",
                }
            )
            print(f"  {name} (no-op handler) rep {rep + 1}/{reps} ... ", end="", flush=True)
            r = run_once(cfg)
            print(f"{r.interior_throughput:.0f} jobs/s")
            runs.append(asdict(r))
        rates = [r["interior_throughput"] for r in runs]
        thread_only[name] = {
            "throughput_median": round(statistics.median(rates), 1),
            "throughput_min": min(rates),
            "throughput_max": max(rates),
            "runs": runs,
        }
    a = thread_only["heartbeat_off"]["throughput_median"]
    b = thread_only["heartbeat_on_never_fires"]["throughput_median"]
    thread_only["cost_pct"] = round((a - b) / a * 100, 1) if a else None
    out["thread_overhead"] = thread_only

    extension = {}
    for name, hb in (("heartbeat_off", 0.0), ("heartbeat_on_100ms", 0.1)):
        cfg = BenchConfig(
            **{
                **asdict(base),
                "workers": 16,
                "handler_sleep": 0.3,
                "heartbeat_interval": hb,
                "jobs_per_worker": 0,
                "min_jobs": 1600,
                "dsn": dsn,
                "label": f"hb-extend-{name}",
            }
        )
        print(f"  {name} (300ms handler) ... ", end="", flush=True)
        r = run_once(cfg)
        d = asdict(r)
        d["extends_per_second"] = (
            round(r.worker_stats["extends"] / r.wall_seconds, 1) if r.wall_seconds else 0.0
        )
        d["extends_per_job"] = (
            round(r.worker_stats["extends"] / r.completed, 2) if r.completed else 0.0
        )
        print(
            f"{r.interior_throughput:.0f} jobs/s, "
            f"{r.worker_stats['extends']} extends ({d['extends_per_second']:.0f}/s)"
        )
        extension[name] = d
    out["extension_cost"] = extension
    return out


def fidelity_check(workers: int, base: BenchConfig, dsn: str | None = None) -> dict[str, Any]:
    """Measure the same point with the bench loop and with the product worker.

    bench/worker.py mirrors Worker.run_once so it can time the calls separately.
    This check makes that claim falsifiable instead of asserted.
    """
    out = {}
    for name, real in (("bench_worker", False), ("product_worker", True)):
        cfg = BenchConfig(
            **{
                **asdict(base),
                "workers": workers,
                "real_worker": real,
                "dsn": dsn,
                "label": f"fidelity-{name}",
            }
        )
        print(f"  {name} ... ", end="", flush=True)
        r = run_once(cfg)
        print(f"{r.interior_throughput:.0f} jobs/s")
        out[name] = asdict(r)
    a = out["bench_worker"]["interior_throughput"]
    b = out["product_worker"]["interior_throughput"]
    out["difference_pct"] = round((a - b) / b * 100, 1) if b else None
    return out


def sync_commit_diagnostic(
    workers: int, base: BenchConfig, dsn: str | None = None
) -> dict[str, Any]:
    """DIAGNOSTIC ONLY. Not a headline number.

    Repeats one point with synchronous_commit off on the worker sessions. This
    is a session setting; the server and the Compose file are untouched, and
    nothing durable changes. If throughput jumps severalfold, WAL fsync was the
    wall. If it does not, the wall is somewhere else.
    """
    out = {}
    for name, sync in (("synchronous_commit_on", "on"), ("synchronous_commit_off", "off")):
        cfg = BenchConfig(
            **{
                **asdict(base),
                "workers": workers,
                "sync_commit": sync,
                "dsn": dsn,
                "label": f"diag-{name}",
            }
        )
        print(f"  {name} ... ", end="", flush=True)
        r = run_once(cfg)
        print(f"{r.interior_throughput:.0f} jobs/s")
        out[name] = asdict(r)
    on = out["synchronous_commit_on"]["interior_throughput"]
    off = out["synchronous_commit_off"]["interior_throughput"]
    out["speedup"] = round(off / on, 2) if on else None
    return out


def steady_state(
    workers: int, capacity: float, base: BenchConfig, dsn: str | None = None
) -> dict[str, Any]:
    """End-to-end latency at fixed arrival rates below capacity.

    In a batch drain, end-to-end latency is mostly backlog residency and says
    little. Here jobs arrive at a set rate, so the percentiles describe how long
    a job actually waits when the system is not saturated.

    ``capacity`` must be the *sustainable* rate, not the drain rate. Draining
    costs two commits per job and enqueueing costs a third, so holding a steady
    state needs ``3 x arrival`` commits per second. Feeding at 90% of the drain
    rate asks for more commits than the machine has and the arrival rate simply
    falls short, which the caller sees as a note on the run rather than as a
    latency number that quietly describes a different experiment.
    """
    out = {"capacity_used": capacity}
    for frac in (0.5, 0.9):
        rate = round(capacity * frac)
        cfg = BenchConfig(
            **{
                **asdict(base),
                "workers": workers,
                "jobs_per_worker": 0,
                "min_jobs": max(400, int(rate * 12)),
                "dsn": dsn,
                "timeout": 180.0,
                "label": f"steady-{int(frac * 100)}",
            }
        )
        print(f"  arrival {rate}/s ({frac:.0%} of capacity) ... ", end="", flush=True)
        r = run_once(cfg, arrival_rate=rate)
        print(f"e2e p50={r.e2e_latency_ms['p50']}ms p99={r.e2e_latency_ms['p99']}ms")
        out[f"{int(frac * 100)}pct_of_capacity"] = {"arrival_rate": rate, "run": asdict(r)}
    return out


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------


def _write(out_dir: Path, name: str, payload: Any) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / name
    path.write_text(json.dumps(payload, indent=2, default=str))
    return path


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Conveyor load test")
    p.add_argument(
        "mode", choices=["sweep", "probe", "heartbeat", "steady", "fidelity", "diagnostic", "all"]
    )
    p.add_argument("--workers", default=",".join(str(w) for w in DEFAULT_WORKER_COUNTS))
    p.add_argument("--reps", type=int, default=3)
    p.add_argument(
        "--target-seconds",
        type=float,
        default=12.0,
        help="each sweep run is sized by a pilot to last about this long",
    )
    p.add_argument("--jobs-per-worker", type=int, default=900)
    p.add_argument("--min-jobs", type=int, default=2000)
    p.add_argument("--arrival-rate", type=float, default=None)
    p.add_argument("--handler-sleep", type=float, default=0.0)
    p.add_argument("--heartbeat-interval", type=float, default=0.0)
    p.add_argument("--sync-commit", default="on")
    p.add_argument("--dsn", default=None)
    p.add_argument("--out", default="bench/reports")
    args = p.parse_args(argv)

    dsn = args.dsn or dsn_from_env()
    worker_counts = tuple(int(x) for x in args.workers.split(","))
    base = BenchConfig(
        jobs_per_worker=args.jobs_per_worker,
        min_jobs=args.min_jobs,
        handler_sleep=args.handler_sleep,
        heartbeat_interval=args.heartbeat_interval,
        sync_commit=args.sync_commit,
        dsn=dsn,
    )
    out_dir = Path(args.out)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    results: dict[str, Any] = {"started_at": stamp, "dsn_host": dsn.rsplit("@", 1)[-1]}

    with connect(dsn) as conn:
        results["environment"] = bench_env.collect(conn)

    if args.mode in ("sweep", "all"):
        print("enqueue throughput (product path, one commit per job, one connection):")
        with connect(dsn) as conn:
            _guard_and_reset(conn)
            results["enqueue"] = measure_enqueue(conn, 3000, "enqueue-measure")
            _reset(conn)
        print(f"  {results['enqueue']['enqueues_per_second']:.0f} enqueues/s")
        print("sweep:")
        results["sweep"] = sweep(
            worker_counts, args.reps, base, dsn, target_seconds=args.target_seconds
        )
    if args.mode in ("probe", "all"):
        print("commit-ceiling probe:")
        results["commit_probe"] = []
        for c in worker_counts:
            r = commit_probe(c, dsn=dsn)
            print(
                f"  concurrency={c:2d}  {r['commits_per_second']:.0f} commits/s "
                f"(implies {r['implied_job_ceiling_per_second']:.0f} jobs/s)"
            )
            results["commit_probe"].append(r)
    if args.mode in ("fidelity", "all"):
        print("fidelity cross-check:")
        best = _best_workers(results) or max(worker_counts)
        results["fidelity"] = fidelity_check(best, base, dsn)
    if args.mode in ("heartbeat", "all"):
        print("heartbeat cost:")
        best = _best_workers(results) or max(worker_counts)
        results["heartbeat"] = heartbeat_cost(base, dsn, workers=best, reps=args.reps)
    if args.mode in ("diagnostic", "all"):
        print("synchronous_commit diagnostic (NOT a headline number):")
        best = _best_workers(results) or max(worker_counts)
        results["sync_commit_diagnostic"] = sync_commit_diagnostic(best, base, dsn)
    if args.mode in ("steady", "all"):
        print("steady-state latency:")
        best = _best_workers(results) or max(worker_counts)
        cap = args.arrival_rate or _sustainable_capacity(results) or 500.0
        results["steady"] = steady_state(best, cap, base, dsn)

    path = _write(out_dir, f"results-{stamp}.json", results)
    print(f"\nraw results: {path}")
    if "environment" in results:
        env_path = out_dir / "environment.md"
        env_path.write_text(bench_env.render_markdown(results["environment"]))
        print(f"environment: {env_path}")
    return 0


def _best_workers(results: dict[str, Any]) -> int | None:
    points = results.get("sweep")
    if not points:
        return None
    return max(points, key=lambda pt: pt["throughput_median"])["workers"]


def _sustainable_capacity(results: dict[str, Any]) -> float | None:
    """The rate the system can both accept and drain at once.

    Drain throughput alone overstates it: a job in steady state costs three
    commits, one to enqueue and two to run, so the arrival rate is bounded by
    the commit ceiling divided by three as well as by the drain rate.
    """
    drain = _best_throughput(results)
    probe = results.get("commit_probe")
    if not probe:
        return drain
    ceiling = max(p["commits_per_second"] for p in probe)
    return min(drain, ceiling / 3) if drain else ceiling / 3


def _best_throughput(results: dict[str, Any]) -> float | None:
    points = results.get("sweep")
    if not points:
        return None
    return max(pt["throughput_median"] for pt in points)


if __name__ == "__main__":
    sys.exit(main())
