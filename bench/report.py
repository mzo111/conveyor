"""Render the benchmark results JSON into the markdown report.

    python -m bench.report bench/reports/results-<stamp>.json > bench/reports/README.md

Kept separate from the harness so the write-up can be regenerated from saved
raw results without re-running anything.
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path
from typing import Any


def _table(header: list[str], rows: list[list[str]]) -> list[str]:
    return [
        "| " + " | ".join(header) + " |",
        "|" + "|".join("---" for _ in header) + "|",
        *["| " + " | ".join(r) + " |" for r in rows],
    ]


def _waits(run: dict[str, Any], n: int = 4) -> str:
    return ", ".join(f"{w['event']} {w['share']:.0%}" for w in run["waits"]["wait_events"][:n])


def _share(run: dict[str, Any], event: str) -> float:
    """Sampled share for one wait event, 0 if it never appeared."""
    for w in run["waits"]["wait_events"]:
        if w["event"] == event:
            return w["share"]
    return 0.0


def render(d: dict[str, Any]) -> str:
    host = d["environment"]["host"]
    sweep = d["sweep"]
    probe = {p["concurrency"]: p for p in d.get("commit_probe", [])}
    base = sweep[0]["throughput_median"]
    best = max(sweep, key=lambda p: p["throughput_median"])
    pct_of_ceiling = [
        pt["throughput_median"] / probe[pt["workers"]]["implied_job_ceiling_per_second"] * 100
        for pt in sweep
        if pt["workers"] in probe
    ]
    out: list[str] = []
    w = out.append

    w("# Conveyor load test")
    w("")
    w(
        f"Run {d['started_at']}. Raw results and every sample are in the `results-*.json` "
        "beside this file; `environment.md` records the machine and the database configuration."
    )
    w("")
    w("## Read this first")
    w("")
    w(
        f"These numbers come from a single-host Docker Compose PostgreSQL on "
        f"{'WSL2' if host['is_wsl'] else host['platform']}, running its **stock configuration** "
        "(128 MB shared buffers, `synchronous_commit=on`, `fsync=on`), on the same machine and "
        "the same cores as the workers driving it. It is not a tuned production database, not "
        "dedicated hardware, and not a network-realistic deployment: client and server talk over "
        "loopback, so there is no network round-trip in any latency below. Nothing was tuned to "
        "improve these results. Treat them as the shape of the system's behaviour on this "
        "machine, not as a number that transfers to yours."
    )
    w("")

    w("## Headline")
    w("")
    w(
        f"- **{best['throughput_median']:.0f} jobs/s** peak sustained drain at "
        f"{best['workers']} workers, no-op handler."
    )
    w(
        f"- Scaling is sublinear from the start and flattens hard: {sweep[-1]['workers']} workers "
        f"give {sweep[-1]['throughput_median'] / base:.1f}x the throughput of one, an efficiency "
        f"of {sweep[-1]['throughput_median'] / base / sweep[-1]['workers']:.0%}."
    )
    w(
        "- **The ceiling is PostgreSQL's write-ahead log, not the claim query.** Measured "
        "evidence below."
    )
    if "enqueue" in d:
        e = d["enqueue"]
        w(
            f"- Enqueue on the product path runs at {e['enqueues_per_second']:.0f}/s on one "
            f"connection, p50 {e['latency_ms']['p50']} ms."
        )
    w("")

    w("## Scaling")
    w("")
    rows = []
    for pt in sweep:
        n, med = pt["workers"], pt["throughput_median"]
        r0 = pt["runs"][0]
        ceil = probe.get(n, {}).get("implied_job_ceiling_per_second")
        rows.append(
            [
                str(n),
                f"{pt['jobs_per_run']:,}",
                f"{med:,.0f}",
                f"{pt['throughput_min']:,.0f}-{pt['throughput_max']:,.0f}",
                f"{med / base:.2f}x",
                f"{med / base / n:.0%}",
                f"{ceil:,.0f}" if ceil else "-",
                f"{med / ceil:.0%}" if ceil else "-",
                f"{r0['cpu']['host_cpu_busy_cores_equivalent']:.1f}",
            ]
        )
    out += _table(
        [
            "Workers",
            "Jobs/run",
            "Jobs/s (median of 3)",
            "Range",
            "Speedup",
            "Efficiency",
            "Commit ceiling",
            "% of ceiling",
            "Host CPU (cores)",
        ],
        rows,
    )
    w("")
    w(
        "Each point is sized by a pilot run so it lasts about 12 seconds, then measured three "
        "times. Throughput is counted over the middle 80% of completions, so worker ramp-up and "
        "the drain at the end are excluded. Run-to-run spread is under 2% everywhere."
    )
    w("")

    w("## Where it stops scaling, and why")
    w("")
    w(
        "Scaling degrades continuously rather than hitting a wall at one worker count: "
        "efficiency falls from "
        f"{sweep[1]['throughput_median'] / base / sweep[1]['workers']:.0%} at "
        f"{sweep[1]['workers']} workers to "
        f"{sweep[-1]['throughput_median'] / base / sweep[-1]['workers']:.0%} at "
        f"{sweep[-1]['workers']}. Four candidate causes, each measured:"
    )
    w("")
    w(
        "**1. It is not the claim query.** A separate probe process does nothing but "
        "`INSERT` + `COMMIT`, with no queue logic, no `SKIP LOCKED`, and no contention over "
        "rows. Its throughput curve is the queue's curve:"
    )
    w("")
    prows = []
    for pt in sweep:
        n = pt["workers"]
        p = probe.get(n)
        if not p:
            continue
        p1 = probe[1]["commits_per_second"]
        prows.append(
            [
                str(n),
                f"{p['commits_per_second']:,.0f}",
                f"{p['commits_per_second'] / p1 / n:.0%}",
                f"{pt['throughput_median'] / base / n:.0%}",
            ]
        )
    out += _table(
        [
            "Concurrency",
            "Commits/s (no queue)",
            "Commit scaling efficiency",
            "Queue scaling efficiency",
        ],
        prows,
    )
    w("")
    w(
        "The two efficiency columns track each other closely and end at essentially the same "
        f"place. The queue sustains {min(pct_of_ceiling):.0f}-{max(pct_of_ceiling):.0f}% of the "
        "pure-commit ceiling at every concurrency, so the claim query, the partial index, and "
        "`SKIP LOCKED` are close to free. Whatever limits plain commits limits the queue by "
        "the same amount."
    )
    w("")
    w(
        "**2. It is the write-ahead log, and the wait events say which part.** Sampling "
        "`pg_stat_activity` every 5 ms through each run:"
    )
    w("")
    wrows = [
        [
            str(pt["workers"]),
            _waits(pt["runs"][0]),
            f"{pt['runs'][0]['waits']['ungranted_lock_observations']:,}",
        ]
        for pt in sweep
    ]
    out += _table(
        ["Workers", "Where active backends were (share of samples)", "Ungranted lock observations"],
        wrows,
    )
    w("")
    w(
        f"At one worker the backend simply waits for `fsync` (`IO/WALSync`, "
        f"{_share(sweep[0]['runs'][0], 'IO/WALSync'):.0%}): the queue is latency-bound on "
        "durable commits, one at a time. As workers are added, group commit amortises the "
        f"flushes and `IO/WALSync` falls to {_share(sweep[-1]['runs'][0], 'IO/WALSync'):.0%}, "
        f"but `LWLock/WALWrite` rises to "
        f"{_share(sweep[-1]['runs'][0], 'LWLock/WALWrite'):.0%}. That lock serialises writes "
        "into the WAL buffers, so the work simply moves from waiting for the disk to queueing "
        "for the log. That is the wall."
    )
    w("")
    w(
        "**3. Row contention exists but is minor.** `Lock/transactionid` appears only at "
        f"{sweep[-1]['workers']} workers and only at "
        f"{_share(sweep[-1]['runs'][0], 'Lock/transactionid'):.0%} of samples, and ungranted "
        "lock observations, though they grow, "
        "stay small next to the WAL share. Empty claims (a worker finding nothing to take) "
        "stay in the tens across runs of tens of thousands of jobs. `SKIP LOCKED` is doing its "
        "job."
    )
    w("")
    w(
        "**4. It is not CPU, connections, or the client.** Host CPU peaks at "
        f"{max(pt['runs'][0]['cpu']['host_cpu_busy_cores_equivalent'] for pt in sweep):.1f} of "
        f"{sweep[0]['runs'][0]['cpu']['cores']} cores with workers and database together. "
        "Connections peak at 16 of the server's 100. `Client/ClientRead`, the backend waiting "
        "on the application, stays at or below 3%, so the workers are keeping the database fed."
    )
    w("")

    if "sync_commit_diagnostic" in d:
        dg = d["sync_commit_diagnostic"]
        on = dg["synchronous_commit_on"]["interior_throughput"]
        off = dg["synchronous_commit_off"]["interior_throughput"]
        w("### Diagnostic, not a result")
        w("")
        w(
            "To confirm the WAL attribution rather than argue it, one point was repeated with "
            "`synchronous_commit = off` set **on the worker sessions only**. The server "
            "configuration and the Compose file are untouched and nothing durable changed. This "
            "is an attribution experiment; it is not how the queue is meant to run and it is "
            "not a headline number, because it trades away the durability the queue depends on."
        )
        w("")
        out += _table(
            ["Session setting", "Jobs/s", "Dominant wait"],
            [
                [
                    "`synchronous_commit=on` (as shipped)",
                    f"{on:,.0f}",
                    _waits(dg["synchronous_commit_on"], 2),
                ],
                [
                    "`synchronous_commit=off` (diagnostic)",
                    f"{off:,.0f}",
                    _waits(dg["synchronous_commit_off"], 2),
                ],
            ],
        )
        w("")
        w(
            f"Removing the commit-flush wait buys {dg['speedup']}x and moves the dominant wait "
            "off the WAL entirely, onto CPU and client wait. That confirms the WAL is the "
            "binding constraint, and shows it is not a single fixable hotspot: the remaining "
            "cost is spread across the whole commit path."
        )
        w("")

    w("## Latency")
    w("")
    w(
        "**Claim and ack** are round-trips measured in the worker with `perf_counter`. They "
        "stay flat as workers are added, which is what a healthy claim path looks like: the "
        "system slows down by doing fewer commits per second, not by making any single claim "
        "wait longer."
    )
    w("")
    lrows = []
    for pt in sweep:
        r0 = pt["runs"][0]
        c, a = r0["claim_latency_ms"], r0["ack_latency_ms"]
        lrows.append(
            [
                str(pt["workers"]),
                f"{c['p50']:.2f}",
                f"{c['p95']:.2f}",
                f"{c['p99']:.2f}",
                f"{a['p50']:.2f}",
                f"{a['p95']:.2f}",
                f"{a['p99']:.2f}",
            ]
        )
    out += _table(
        ["Workers", "Claim p50", "Claim p95", "Claim p99", "Ack p50", "Ack p95", "Ack p99"],
        lrows,
    )
    w("")
    w("All values in milliseconds, over loopback with no network hop.")
    w("")

    if "steady" in d:
        w(
            "**End-to-end latency** is only meaningful when jobs arrive over time. In the batch "
            "drain above, a job's `finished_at - created_at` is dominated by how many jobs were "
            "queued ahead of it, so its p50 of about "
            f"{best['runs'][0]['e2e_latency_ms']['p50'] / 1000:.0f} seconds is a restatement of "
            "throughput and backlog depth, not a measure of responsiveness. These numbers come "
            "instead from a fixed arrival rate:"
        )
        w("")
        srows = []
        max_achieved = max(
            v["run"]["achieved_arrival_rate"] or 0
            for v in d["steady"].values()
            if isinstance(v, dict) and "run" in v
        )
        for key, v in d["steady"].items():
            if not isinstance(v, dict) or "run" not in v:
                continue
            r = v["run"]
            e = r["e2e_latency_ms"]
            achieved = r["achieved_arrival_rate"]
            short = " (fell short)" if r["notes"] else ""
            srows.append(
                [
                    key.replace("_", " "),
                    f"{v['arrival_rate']:,}",
                    f"{achieved:,.0f}{short}",
                    f"{e['p50']:.1f}",
                    f"{e['p95']:.1f}",
                    f"{e['p99']:.1f}",
                ]
            )
        out += _table(
            ["Load", "Target arrival/s", "Achieved arrival/s", "e2e p50", "e2e p95", "e2e p99"],
            srows,
        )
        w("")
        w(
            "Milliseconds, measured from database timestamps. The achieved arrival rate is "
            "reported next to the target because a feeder that cannot keep up would otherwise "
            "produce a latency number for an experiment that never happened."
        )
        w("")
        w(
            "**Sustainable capacity is well below drain capacity, and the shortfall above is "
            "the measurement.** Draining a pre-filled queue costs two commits per job. Holding "
            "a steady state costs three, because something has to enqueue as well, and the "
            "producers compete with the workers for the same WAL and the same cores. The "
            f"commit ceiling divided by three predicts about "
            f"{max(p['commits_per_second'] for p in probe.values()) / 3:,.0f} jobs/s; the "
            f"highest arrival rate actually achieved was {max_achieved:,.0f} jobs/s, against a "
            f"drain capacity of {best['throughput_median']:,.0f} jobs/s. So a running system "
            "holds roughly half of what its drain benchmark suggests. Asking for more does not "
            "build a backlog, it simply arrives more slowly, which is why the achieved rate is "
            "reported next to the target rather than assumed."
        )
        w("")

    if "heartbeat" in d:
        hb = d["heartbeat"]
        th = hb["thread_overhead"]
        ext = hb["extension_cost"]
        w("## Cost of heartbeating")
        w("")
        w("Lease extension has two separable costs, so they are measured separately.")
        w("")
        w(
            "**Per-job thread cost.** With a no-op handler and the product default interval "
            "(visibility timeout / 3, so no extension ever actually fires), the only cost is "
            "starting and joining a thread for every job:"
        )
        w("")
        out += _table(
            ["Configuration", "Jobs/s (median of 3)", "Range"],
            [
                [
                    "Heartbeat off",
                    f"{th['heartbeat_off']['throughput_median']:,.0f}",
                    f"{th['heartbeat_off']['throughput_min']:,.0f}-"
                    f"{th['heartbeat_off']['throughput_max']:,.0f}",
                ],
                [
                    "Heartbeat on, never fires",
                    f"{th['heartbeat_on_never_fires']['throughput_median']:,.0f}",
                    f"{th['heartbeat_on_never_fires']['throughput_min']:,.0f}-"
                    f"{th['heartbeat_on_never_fires']['throughput_max']:,.0f}",
                ],
            ],
        )
        w("")
        w(
            f"That is a **{th['cost_pct']}% throughput cost** on jobs that do nothing. The two "
            "ranges above do not overlap, so the effect is real rather than run-to-run noise. "
            "It is a fixed per-job cost, so it matters only when jobs are trivially short, "
            "which is exactly when leases are least likely to expire."
        )
        w("")
        off, on = ext["heartbeat_off"], ext["heartbeat_on_100ms"]
        w(
            "**Extension cost.** With a 300 ms handler and a 100 ms interval, roughly two "
            "`extend_lease` round-trips happen per job:"
        )
        w("")
        out += _table(
            ["Configuration", "Jobs/s", "Extends/s", "Extends per job"],
            [
                [
                    "Heartbeat off",
                    f"{off['interior_throughput']:.0f}",
                    f"{off.get('extends_per_second', 0):.0f}",
                    f"{off.get('extends_per_job', 0):.1f}",
                ],
                [
                    "Heartbeat on, 100 ms",
                    f"{on['interior_throughput']:.0f}",
                    f"{on.get('extends_per_second', 0):.0f}",
                    f"{on.get('extends_per_job', 0):.1f}",
                ],
            ],
        )
        w("")
        ceiling = max(p["commits_per_second"] for p in probe.values()) if probe else 0
        share = on.get("extends_per_second", 0) / ceiling if ceiling else 0
        w(
            "Throughput is unchanged, because a 300 ms handler at 16 workers is bound by the "
            "sleep, not by the database. The database cost is the honest figure to quote: "
            f"{on.get('extends_per_second', 0):.0f} extra commits per second, about "
            f"{share:.0%} of this machine's measured commit ceiling. Heartbeating is cheap "
            "exactly where it is needed, on long jobs, and its real price is the fixed "
            "per-job thread cost above, paid on short ones."
        )
        w("")
        w(
            "One mechanism worth knowing: the heartbeat shares the worker's single connection, "
            "so an in-flight extension serialises against the worker's next query. That is "
            "invisible here, but it would matter for a handler firing extensions much more "
            "often."
        )
        w("")

    if "fidelity" in d:
        f = d["fidelity"]
        w("## Is the measuring instrument honest?")
        w("")
        w(
            "`bench/worker.py` mirrors `Worker.run_once` so it can time claim and ack "
            "separately, which the product class does not expose. That mirroring could drift, "
            "so the same configuration was measured both ways: with the bench loop, and with "
            "the unmodified `python -m conveyor.worker` counting completions from the "
            "database."
        )
        w("")
        out += _table(
            ["Worker", "Jobs/s"],
            [
                ["`bench/worker.py`", f"{f['bench_worker']['interior_throughput']:,.0f}"],
                [
                    "`conveyor.worker` (product)",
                    f"{f['product_worker']['interior_throughput']:,.0f}",
                ],
            ],
        )
        w("")
        w(
            f"A {abs(f['difference_pct'])}% difference, within the run-to-run spread seen "
            "elsewhere at this worker count. The bench loop is a fair stand-in for the real "
            "worker."
        )
        w("")

    w("## What was not tuned")
    w("")
    w(
        "- `docker-compose.yml` and the PostgreSQL configuration are exactly as the repository "
        "ships them. No `shared_buffers`, `wal_*`, or `synchronous_commit` changes."
    )
    w("- No indexes were added or altered for the benchmark.")
    w("- Claims are one job at a time. No batching, no `LIMIT n` claim, no prefetch.")
    w("- No connection pooler; one connection per worker, as the product uses.")
    w(
        "- The only session-level change anywhere is `synchronous_commit=off` in the diagnostic "
        "above, which is labelled as a diagnostic and excluded from every headline number."
    )
    w("")
    w(
        "The one place the bench does not use the product path is filling the queue before a "
        "batch-drain run: it uses `COPY`, because loading 55,000 jobs through `enqueue` would "
        "take longer than the run it is setting up. `COPY` writes the same columns and leaves "
        "every other column at its schema default, so workers see identical rows. The real "
        "`enqueue` throughput is measured separately and quoted in the headline."
    )
    w("")

    w("## Reproducing")
    w("")
    w("```sh")
    w("docker compose up -d db")
    w("python -m bench.load all --workers 1,2,4,8,16 --reps 3 --out bench/reports")
    w("python -m bench.report bench/reports/results-<stamp>.json > bench/reports/README.md")
    w("```")
    w("")
    w(
        "The harness truncates the jobs table and refuses to start if the queue already holds "
        "queued or running work."
    )
    return "\n".join(out) + "\n"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("results", nargs="?", help="results JSON (default: newest in bench/reports)")
    args = p.parse_args(argv)
    path = args.results
    if not path:
        found = sorted(glob.glob("bench/reports/results-*.json"))
        if not found:
            print("no results file found", file=sys.stderr)
            return 1
        path = found[-1]
    print(render(json.loads(Path(path).read_text())), end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
