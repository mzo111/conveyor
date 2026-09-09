# Conveyor load test

Run 20260907T084259Z. Raw results and every sample are in the `results-*.json` beside this file; `environment.md` records the machine and the database configuration.

## Read this first

These numbers come from a single-host Docker Compose PostgreSQL on WSL2, running its **stock configuration** (128 MB shared buffers, `synchronous_commit=on`, `fsync=on`), on the same machine and the same cores as the workers driving it. It is not a tuned production database, not dedicated hardware, and not a network-realistic deployment: client and server talk over loopback, so there is no network round-trip in any latency below. Nothing was tuned to improve these results. Treat them as the shape of the system's behaviour on this machine, not as a number that transfers to yours.

## Headline

- **4210 jobs/s** peak sustained drain at 16 workers, no-op handler.
- Scaling is sublinear from the start and flattens hard: 16 workers give 7.6x the throughput of one, an efficiency of 47%.
- **The ceiling is PostgreSQL's write-ahead log, not the claim query.** Measured evidence below.
- Enqueue on the product path runs at 1059/s on one connection, p50 0.887 ms.

## Scaling

| Workers | Jobs/run | Jobs/s (median of 3) | Range | Speedup | Efficiency | Commit ceiling | % of ceiling | Host CPU (cores) |
|---|---|---|---|---|---|---|---|---|
| 1 | 6,538 | 557 | 553-559 | 1.00x | 100% | 605 | 92% | 0.7 |
| 2 | 12,163 | 966 | 952-1,002 | 1.73x | 87% | 997 | 97% | 1.2 |
| 4 | 20,367 | 1,664 | 1,653-1,682 | 2.99x | 75% | 1,979 | 84% | 1.9 |
| 8 | 34,130 | 2,777 | 2,765-2,814 | 4.98x | 62% | 3,315 | 84% | 3.4 |
| 16 | 53,607 | 4,210 | 4,130-4,223 | 7.55x | 47% | 4,716 | 89% | 6.5 |

Each point is sized by a pilot run so it lasts about 12 seconds, then measured three times. Throughput is counted over the middle 80% of completions, so worker ramp-up and the drain at the end are excluded. Run-to-run spread is under 2% everywhere.

## Where it stops scaling, and why

Scaling degrades continuously rather than hitting a wall at one worker count: efficiency falls from 87% at 2 workers to 47% at 16. Four candidate causes, each measured:

**1. It is not the claim query.** A separate probe process does nothing but `INSERT` + `COMMIT`, with no queue logic, no `SKIP LOCKED`, and no contention over rows. Its throughput curve is the queue's curve:

| Concurrency | Commits/s (no queue) | Commit scaling efficiency | Queue scaling efficiency |
|---|---|---|---|
| 1 | 1,210 | 100% | 100% |
| 2 | 1,994 | 82% | 87% |
| 4 | 3,957 | 82% | 75% |
| 8 | 6,631 | 69% | 62% |
| 16 | 9,432 | 49% | 47% |

The two efficiency columns track each other closely and end at essentially the same place. The queue sustains 84-97% of the pure-commit ceiling at every concurrency, so the claim query, the partial index, and `SKIP LOCKED` are close to free. Whatever limits plain commits limits the queue by the same amount.

**2. It is the write-ahead log, and the wait events say which part.** Sampling `pg_stat_activity` every 5 ms through each run:

| Workers | Where active backends were (share of samples) | Ungranted lock observations |
|---|---|---|
| 1 | IO/WALSync 82%, CPU (running, no wait) 18%, Client/ClientRead 0%, IO/WALWrite 0% | 0 |
| 2 | IO/WALSync 72%, CPU (running, no wait) 17%, LWLock/WALWrite 10%, IO/WALWrite 0% | 1 |
| 4 | LWLock/WALWrite 50%, IO/WALSync 34%, CPU (running, no wait) 14%, Client/ClientRead 2% | 1 |
| 8 | LWLock/WALWrite 65%, IO/WALSync 17%, CPU (running, no wait) 13%, Client/ClientRead 3% | 78 |
| 16 | LWLock/WALWrite 67%, CPU (running, no wait) 12%, IO/WALSync 9%, Lock/transactionid 7% | 1,317 |

At one worker the backend simply waits for `fsync` (`IO/WALSync`, 82%): the queue is latency-bound on durable commits, one at a time. As workers are added, group commit amortises the flushes and `IO/WALSync` falls to 9%, but `LWLock/WALWrite` rises to 67%. That lock serialises writes into the WAL buffers, so the work simply moves from waiting for the disk to queueing for the log. That is the wall.

**3. Row contention exists but is minor.** `Lock/transactionid` appears only at 16 workers and only at 7% of samples, and ungranted lock observations, though they grow, stay small next to the WAL share. Empty claims (a worker finding nothing to take) stay in the tens across runs of tens of thousands of jobs. `SKIP LOCKED` is doing its job.

**4. It is not CPU, connections, or the client.** Host CPU peaks at 6.5 of 24 cores with workers and database together. Connections peak at 16 of the server's 100. `Client/ClientRead`, the backend waiting on the application, stays at or below 3%, so the workers are keeping the database fed.

### Diagnostic, not a result

To confirm the WAL attribution rather than argue it, one point was repeated with `synchronous_commit = off` set **on the worker sessions only**. The server configuration and the Compose file are untouched and nothing durable changed. This is an attribution experiment; it is not how the queue is meant to run and it is not a headline number, because it trades away the durability the queue depends on.

| Session setting | Jobs/s | Dominant wait |
|---|---|---|
| `synchronous_commit=on` (as shipped) | 4,402 | LWLock/WALWrite 71%, CPU (running, no wait) 11% |
| `synchronous_commit=off` (diagnostic) | 7,411 | CPU (running, no wait) 53%, Client/ClientRead 27% |

Removing the commit-flush wait buys 1.68x and moves the dominant wait off the WAL entirely, onto CPU and client wait. That confirms the WAL is the binding constraint, and shows it is not a single fixable hotspot: the remaining cost is spread across the whole commit path.

## Latency

**Claim and ack** are round-trips measured in the worker with `perf_counter`. They stay flat as workers are added, which is what a healthy claim path looks like: the system slows down by doing fewer commits per second, not by making any single claim wait longer.

| Workers | Claim p50 | Claim p95 | Claim p99 | Ack p50 | Ack p95 | Ack p99 |
|---|---|---|---|---|---|---|
| 1 | 0.90 | 1.21 | 1.60 | 0.81 | 1.12 | 1.42 |
| 2 | 0.95 | 1.46 | 1.62 | 0.91 | 1.44 | 1.61 |
| 4 | 1.35 | 1.72 | 2.40 | 0.99 | 1.58 | 2.24 |
| 8 | 1.52 | 1.82 | 2.31 | 1.39 | 1.77 | 2.27 |
| 16 | 1.81 | 2.52 | 2.97 | 1.74 | 2.73 | 3.27 |

All values in milliseconds, over loopback with no network hop.

**End-to-end latency** is only meaningful when jobs arrive over time. In the batch drain above, a job's `finished_at - created_at` is dominated by how many jobs were queued ahead of it, so its p50 of about 8 seconds is a restatement of throughput and backlog depth, not a measure of responsiveness. These numbers come instead from a fixed arrival rate:

| Load | Target arrival/s | Achieved arrival/s | e2e p50 | e2e p95 | e2e p99 |
|---|---|---|---|---|---|
| 50pct of capacity | 1,572 | 1,725 | 4.0 | 7.2 | 19.6 |
| 90pct of capacity | 2,830 | 2,228 (fell short) | 5.4 | 11.0 | 16.6 |

Milliseconds, measured from database timestamps. The achieved arrival rate is reported next to the target because a feeder that cannot keep up would otherwise produce a latency number for an experiment that never happened.

**Sustainable capacity is well below drain capacity, and the shortfall above is the measurement.** Draining a pre-filled queue costs two commits per job. Holding a steady state costs three, because something has to enqueue as well, and the producers compete with the workers for the same WAL and the same cores. The commit ceiling divided by three predicts about 3,144 jobs/s; the highest arrival rate actually achieved was 2,228 jobs/s, against a drain capacity of 4,210 jobs/s. So a running system holds roughly half of what its drain benchmark suggests. Asking for more does not build a backlog, it simply arrives more slowly, which is why the achieved rate is reported next to the target rather than assumed.

## Cost of heartbeating

Lease extension has two separable costs, so they are measured separately.

**Per-job thread cost.** With a no-op handler and the product default interval (visibility timeout / 3, so no extension ever actually fires), the only cost is starting and joining a thread for every job:

| Configuration | Jobs/s (median of 3) | Range |
|---|---|---|
| Heartbeat off | 4,174 | 4,172-4,198 |
| Heartbeat on, never fires | 3,930 | 3,928-3,943 |

That is a **5.8% throughput cost** on jobs that do nothing. The two ranges above do not overlap, so the effect is real rather than run-to-run noise. It is a fixed per-job cost, so it matters only when jobs are trivially short, which is exactly when leases are least likely to expire.

**Extension cost.** With a 300 ms handler and a 100 ms interval, roughly two `extend_lease` round-trips happen per job:

| Configuration | Jobs/s | Extends/s | Extends per job |
|---|---|---|---|
| Heartbeat off | 53 | 0 | 0.0 |
| Heartbeat on, 100 ms | 53 | 105 | 2.0 |

Throughput is unchanged, because a 300 ms handler at 16 workers is bound by the sleep, not by the database. The database cost is the honest figure to quote: 105 extra commits per second, about 1% of this machine's measured commit ceiling. Heartbeating is cheap exactly where it is needed, on long jobs, and its real price is the fixed per-job thread cost above, paid on short ones.

One mechanism worth knowing: the heartbeat shares the worker's single connection, so an in-flight extension serialises against the worker's next query. That is invisible here, but it would matter for a handler firing extensions much more often.

## Is the measuring instrument honest?

`bench/worker.py` mirrors `Worker.run_once` so it can time claim and ack separately, which the product class does not expose. That mirroring could drift, so the same configuration was measured both ways: with the bench loop, and with the unmodified `python -m conveyor.worker` counting completions from the database.

| Worker | Jobs/s |
|---|---|
| `bench/worker.py` | 4,185 |
| `conveyor.worker` (product) | 4,184 |

A 0.0% difference, within the run-to-run spread seen elsewhere at this worker count. The bench loop is a fair stand-in for the real worker.

## What was not tuned

- `docker-compose.yml` and the PostgreSQL configuration are exactly as the repository ships them. No `shared_buffers`, `wal_*`, or `synchronous_commit` changes.
- No indexes were added or altered for the benchmark.
- Claims are one job at a time. No batching, no `LIMIT n` claim, no prefetch.
- No connection pooler; one connection per worker, as the product uses.
- The only session-level change anywhere is `synchronous_commit=off` in the diagnostic above, which is labelled as a diagnostic and excluded from every headline number.

The one place the bench does not use the product path is filling the queue before a batch-drain run: it uses `COPY`, because loading 55,000 jobs through `enqueue` would take longer than the run it is setting up. `COPY` writes the same columns and leaves every other column at its schema default, so workers see identical rows. The real `enqueue` throughput is measured separately and quoted in the headline.

## Reproducing

```sh
docker compose up -d db
python -m bench.load all --workers 1,2,4,8,16 --reps 3 --out bench/reports
python -m bench.report bench/reports/results-<stamp>.json > bench/reports/README.md
```

The harness truncates the jobs table and refuses to start if the queue already holds queued or running work.
