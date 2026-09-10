# Conveyor

A job queue built on a single PostgreSQL table, with at-least-once delivery, visibility
leases, exponential-backoff retries, a dead-letter path, and a pip-installable client SDK.
It is deliberately small — the queue is `SELECT ... FOR UPDATE SKIP LOCKED` plus a deadline
column — and everything it claims about correctness and performance is backed by a chaos
harness and a load benchmark whose reports live in this repository.

## Quickstart

```sh
docker compose up -d db                              # Postgres 16 on localhost:5433
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
pip install .                                        # the conveyor_client SDK
```

Write a handler. The worker hands your function the whole job row, and the SDK stores the
handler name in a small envelope alongside your payload, so unwrap it at the boundary:

```python
# myapp/handlers.py
def send_welcome(payload):
    print("welcome", payload["user_id"])


# `python -m conveyor.worker --handler myapp.handlers:entrypoint`
def entrypoint(job):
    send_welcome(job.payload["payload"])
```

Enqueue from anywhere with a connection string:

```python
from conveyor_client import Client

with Client("postgresql://conveyor:conveyor@localhost:5433/conveyor") as client:
    client.enqueue("send_welcome", {"user_id": 42})
```

Run a worker, and a reaper to return expired leases to the queue:

```sh
python -m conveyor.worker --handler myapp.handlers:entrypoint
python -m conveyor.reaper --interval 5               # in another shell
```

`DATABASE_URL` overrides the default DSN. A database created before the `cancelled` status
existed needs it added once — Compose applies `db/schema.sql` only on first initialisation:

```sh
psql "$DATABASE_URL" -f db/migrations/001_add_cancelled_status.sql
```

Jobs that exhaust their attempts are inspectable and replayable:

```sh
python -m conveyor.deadletter list
python -m conveyor.deadletter show 42
python -m conveyor.deadletter replay 42 [--extra-attempts N]
```

## Architecture

There is no broker process. The `jobs` table *is* the queue, and every participant is a plain
Postgres client. That is the whole design: coordination is delegated to the row lock, and
liveness to a timestamp column.

```mermaid
flowchart LR
    SDK["<b>conveyor_client.Client</b><br/>enqueue · get · cancel<br/><i>pooled, no queue internals</i>"]

    JOBS[("<b>jobs</b><br/>─────────────<br/>partial idx (run_at, id)<br/>WHERE status='queued'<br/><br/>partial idx (visibility_deadline)<br/>WHERE status='running'<br/><br/>unique idx (idempotency_key)")]

    W["<b>Worker</b> × N<br/>claim → handler → ack/nack<br/>heartbeat every timeout/3"]
    R["<b>Reaper</b><br/>sweeps expired leases"]
    DL["<b>deadletter</b> CLI<br/>list · show · replay"]

    SDK -->|"INSERT … ON CONFLICT (idempotency_key) DO NOTHING"| JOBS
    JOBS -->|"SELECT … FOR UPDATE SKIP LOCKED<br/>+ UPDATE → running, attempts += 1"| W
    W -->|"ack / nack — fenced on attempts"| JOBS
    W -.->|"extend_lease — fenced on attempts"| JOBS
    R -->|"status='running' AND visibility_deadline &lt; now()"| JOBS
    JOBS --> DL
```

A job's whole life is one row moving through five states:

```mermaid
stateDiagram-v2
    [*] --> queued: enqueue
    queued --> running: claim
    running --> succeeded: ack(attempt)
    running --> queued: nack(attempt)<br/>retries remaining
    running --> dead: nack(attempt)<br/>attempts ≥ max
    running --> queued: reaper: lease expired<br/>retries remaining
    running --> dead: reaper: lease expired<br/>attempts ≥ max
    queued --> cancelled: cancel (SDK)
    dead --> queued: deadletter replay
    succeeded --> [*]
    cancelled --> [*]
```

Three mechanisms carry the weight:

**Claiming is exclusive because of the row lock, not the snapshot.** The claim is one
statement in one transaction: an inner `SELECT ... FOR UPDATE SKIP LOCKED` takes a row-level
lock, and the enclosing `UPDATE` flips the row to `running`. Under READ COMMITTED two workers
can both hold snapshots in which the same row is `queued`, so isolation alone would not be
enough; the lock is what turns "both saw it" into "one gets it". `SKIP LOCKED` only changes
the loser's behaviour from blocking to moving on. The full argument is in the docstring of
`claim` in [`conveyor/queue.py`](conveyor/queue.py).

**`running` is a lease, not a lock.** The claim commits *before* the handler runs, and the
handler runs outside any transaction. What keeps other workers off the job is
`visibility_deadline`, a timestamp. If it passes without an ack, the reaper requeues the job.
This is the decision that makes the queue scale and that makes delivery at-least-once; both
consequences are argued in the trade-offs below.

**`attempts` is a fencing token.** It is incremented at claim time, so it is unique per claim,
and `ack`, `nack` and `extend_lease` all carry it in their `WHERE` clause. A worker whose lease
was reaped cannot write over a newer attempt's state — its update matches zero rows and is
discarded.

## Measured results

Everything below was produced by [`bench/`](bench) and [`chaos/`](chaos) on one machine, and
every number links to the file that produced it. Read the caveat before the numbers.

### Hardware and configuration

| | |
|---|---|
| CPU | 13th Gen Intel Core i7-13700KF, 24 logical cores |
| Memory | 15.5 GiB |
| Kernel | 5.15.133.1-microsoft-standard-WSL2 (**WSL2**) |
| Disk | 1,006.9 GiB total, 943.7 GiB free |
| Docker | 29.2.1, `overlayfs` storage driver |
| PostgreSQL | 16.15 in Docker Compose, **stock configuration** |
| Key settings | `shared_buffers` 128 MB, `synchronous_commit=on`, `fsync=on`, `max_wal_size` 1 GB, `max_connections` 100 |
| Python / psycopg | 3.12.3 / 3.3.5 |

Full detail, collected automatically rather than hand-written, is in
[`bench/reports/environment.md`](bench/reports/environment.md).

**That file is rewritten by every run and carries no run stamp**, so it describes whichever
run finished last — currently the 2026-09-10 commit probe. It is not the provenance for any
particular number above. Each `results-*.json` embeds its own `environment` block, captured
during that run, and that is the one to read when checking what a specific figure was
measured on.

**These numbers do not transfer to your machine.** The database is stock, untuned, running on
WSL2's virtual disk, sharing the same 24 cores with the workers driving it, and talking to
them over loopback — so no latency below includes a network hop, and `fsync` latency is not
what the same SSD would give on bare metal. Nothing was tuned to improve any result. Treat
these as the *shape* of the system's behaviour, not as a score.

### Throughput and scaling

No-op handler, draining a pre-filled queue. Each point is sized by a pilot run to last about
12 seconds, then measured three times; throughput is counted over the middle 80% of
completions so ramp-up and drain are excluded.

**The load test was run twice, three days apart, on the same machine and the same
configuration — and both runs are reported here.** Replacing the first with the second would
throw away what the second run actually established: how far this measurement moves between
sittings.

| Workers | 2026-09-07 median | range | 2026-09-10 median | range | Δ median |
|---|---|---|---|---|---|
| 1 | 557 | 553–559 | 609 | 544–620 | +9.2% |
| 2 | 966 | 952–1,002 | 981 | 976–1,111 | +1.6% |
| 4 | 1,664 | 1,653–1,682 | 1,858 | 1,652–1,912 | +11.7% |
| 8 | 2,777 | 2,765–2,814 | 2,828 | 2,805–3,166 | +1.8% |
| **16** | **4,210** | 4,130–4,223 | **4,733** | 4,202–4,775 | **+12.4%** |

Sources: [scaling table](bench/reports/README.md#scaling) and
[`results-20260907T084259Z.json`](bench/reports/results-20260907T084259Z.json) for the first
run, [`results-20260910T035732Z.json`](bench/reports/results-20260910T035732Z.json) for the
second.

Three things to read off the pair:

- **The medians differ by up to 12%, but every worker count's ranges overlap.** At 16 workers
  the first run spans 4,130–4,223 and the second 4,202–4,775, meeting in 4,202–4,223. The gap
  between medians is wider than either run's own spread would lead you to expect.
- **The second run was about six times noisier.** Intra-run spread (max − min over the
  median) was 1.1–5.2% on 2026-09-07 and 12.1–14.0% on 2026-09-10, at every worker count,
  with nothing changed in the harness, the database or the machine. An earlier version of
  this section claimed spread was "under 2% everywhere"; that was wrong even for the first
  run, whose 2-worker point spread 5.2%.
- **What reproduced is the shape, not the number.** Efficiency at 16 workers was 47% then and
  49% now; speedup 7.55× and 7.77×. Quote the curve. A single throughput figure from this
  benchmark carries roughly ±12% of run-to-run slack on this machine, and any comparison
  smaller than that is noise.

Per-run detail for the second run, alongside its own commit probe
([`results-20260910T040144Z.json`](bench/reports/results-20260910T040144Z.json)):

| Workers | Jobs/s (median of 3) | Range | Speedup | Efficiency | Commit ceiling | % of ceiling | Host CPU |
|---|---|---|---|---|---|---|---|
| 1 | 609 | 544–620 | 1.00× | 100% | 602 | 101% | 0.6 cores |
| 2 | 981 | 976–1,111 | 1.61× | 81% | 975 | 101% | 1.1 cores |
| 4 | 1,858 | 1,652–1,912 | 3.05× | 76% | 1,917 | 97% | 1.7 cores |
| 8 | 2,828 | 2,805–3,166 | 4.64× | 58% | 3,405 | 83% | 3.0 cores |
| **16** | **4,733** | 4,202–4,775 | **7.77×** | **49%** | 4,858 | 97% | 5.9 cores |

The commit-ceiling relationship held: the queue sustained 83–101% of the pure-commit ceiling,
against 84–97% in the first run. Two points read just over 100% because the sweep and the
probe are separate runs four minutes apart, not two halves of one — at this noise level a
ratio of two independent runs can cross 1.0, and rounding it down to 100% would be tidying.

The first run's equivalent table is in
[scaling](bench/reports/README.md#scaling); its 16-worker point is 4,210 jobs/s at 47%
efficiency and 89% of a 4,716/s ceiling, on 6.5 cores.

Scaling degrades continuously rather than hitting a wall in both runs: efficiency falls from
87% (81% in the second run) at two workers to 47–49% at sixteen. Enqueue on the product path
runs at [1,059/s on one connection, p50 0.887 ms](bench/reports/README.md#headline).

**The sweep measures the queue, not a full worker process.** Every point above runs with
`real_worker: false` — the driver is `bench/worker.py`, a loop that mirrors
`Worker.run_once` so it can time claim and ack separately, which the product class does not
expose. It is not the shipped worker, so these figures exclude whatever a real worker process
costs beyond the claim/ack path. The size of that gap was measured rather than assumed: the
same configuration run through the unmodified `python -m conveyor.worker`, counting
completions from the database, gave 4,184 jobs/s against the bench loop's 4,185 — a 0.0%
difference, inside the run-to-run spread above
([instrument check](bench/reports/README.md#is-the-measuring-instrument-honest)). The chaos
tests, by contrast, drive real worker subprocesses throughout.

Claim and ack latency stays flat as workers are added, which is what a healthy claim path looks
like — the system slows by doing fewer commits per second, not by making any single claim wait
longer ([latency table](bench/reports/README.md#latency)):

| Workers | Claim p50 | Claim p99 | Ack p50 | Ack p99 |
|---|---|---|---|---|
| 1 | 0.90 ms | 1.60 ms | 0.81 ms | 1.42 ms |
| 16 | 1.81 ms | 2.97 ms | 1.74 ms | 3.27 ms |

### The ceiling is the write-ahead log, not the claim query

This is the load test's actual finding, and it rests on three independent lines of evidence.
All three are in [Where it stops scaling, and why](bench/reports/README.md#where-it-stops-scaling-and-why).

**1. A probe with no queue logic scales the same way.** A separate process does nothing but
`INSERT` + `COMMIT` — no `SKIP LOCKED`, no row contention, no queue at all. Its efficiency
curve tracks the queue's and ends in the same place:

| Concurrency | Commits/s (no queue) | Commit efficiency | Queue efficiency |
|---|---|---|---|
| 1 | 1,210 | 100% | 100% |
| 2 | 1,994 | 82% | 87% |
| 4 | 3,957 | 82% | 75% |
| 8 | 6,631 | 69% | 62% |
| 16 | 9,432 | 49% | 47% |

The queue sustains **84–97% of the pure-commit ceiling at every concurrency**. Whatever limits
plain commits limits the queue by the same amount, so the claim query, the partial index and
`SKIP LOCKED` are close to free.

**2. Wait-event sampling names which part of the WAL.** Sampling `pg_stat_activity` every 5 ms
through each run:

| Workers | Where active backends were | Ungranted locks |
|---|---|---|
| 1 | **IO/WALSync 82%**, CPU 18%, LWLock/WALWrite 0% | 0 |
| 2 | IO/WALSync 72%, CPU 17%, LWLock/WALWrite 10% | 1 |
| 4 | **LWLock/WALWrite 50%**, IO/WALSync 34%, CPU 14% | 1 |
| 8 | **LWLock/WALWrite 65%**, IO/WALSync 17%, CPU 13% | 78 |
| 16 | **LWLock/WALWrite 67%**, CPU 12%, IO/WALSync 9%, Lock/transactionid 7% | 1,317 |

At one worker the backend simply waits for `fsync`: the queue is latency-bound on durable
commits, one at a time. Adding workers lets group commit amortise the flushes — `IO/WALSync`
falls from 82% to 9% — but `LWLock/WALWrite` climbs to 67%. That lock serialises writes into
the WAL buffers. The cost does not disappear; it moves from waiting for the disk to queueing
for the log. That is the wall.

**3. Removing the commit flush moves the bottleneck off the WAL entirely.** One point was
repeated with `synchronous_commit=off` set **on the worker sessions only** — the server config
and Compose file untouched. This is an attribution experiment, not a result, and it is excluded
from every headline number because it trades away the durability the queue depends on
([diagnostic](bench/reports/README.md#diagnostic-not-a-result)):

| Session setting | Jobs/s | Dominant wait |
|---|---|---|
| `synchronous_commit=on` (as shipped) | 4,402 | LWLock/WALWrite 71%, CPU 11% |
| `synchronous_commit=off` (diagnostic) | 7,411 | CPU 53%, Client/ClientRead 27% |

A 1.68× gain, and the dominant wait leaves the WAL for CPU and client wait. That confirms the
attribution and also shows it is not one fixable hotspot: the remaining cost is spread across
the whole commit path.

**What it is not.** Host CPU peaks at 6.5 of 24 cores with workers and database together.
Connections peak at 16 of the server's 100. `Client/ClientRead` — the backend waiting on the
application — stays at or below 3%, so the workers are keeping the database fed. Row contention
is real but minor: `Lock/transactionid` appears only at 16 workers and only in 7% of samples,
and empty claims stay in the tens across runs of tens of thousands of jobs.

### Sustained arrival is about half of drain capacity

This section is all from the 2026-09-07 run, which is the one that carries a steady-state
block; the 2026-09-10 reproduction covered the sweep and the commit probe only.

The 4,210 jobs/s drain figure is exactly that — a *drain* number: a pre-filled queue emptied as fast as possible,
costing two commits per job (claim, ack). A system in steady state costs **three**, because
something has to enqueue as well, and those producers compete with the workers for the same WAL
and the same cores. This is the distinction that makes a drain benchmark misleading if quoted
alone.

The commit ceiling divided by three predicts about 3,144 jobs/s. The highest arrival rate
actually achieved was **2,228 jobs/s**, against that run's drain capacity of 4,210
([end-to-end latency](bench/reports/README.md#latency)):

| Load | Target arrival/s | Achieved arrival/s | e2e p50 | e2e p95 | e2e p99 |
|---|---|---|---|---|---|
| 50% of capacity | 1,572 | 1,725 | 4.0 ms | 7.2 ms | 19.6 ms |
| 90% of capacity | 2,830 | **2,228 (fell short)** | 5.4 ms | 11.0 ms | 16.6 ms |

The shortfall in the second row *is* the measurement. Asking for more than the system can
sustain does not build a backlog — it simply arrives more slowly, which is why the achieved
rate is reported next to the target rather than assumed. **A running system holds roughly half
of what its drain benchmark suggests.**

End-to-end latency is only meaningful under a fixed arrival rate. In the batch drain, a job's
`finished_at - created_at` p50 of about 8 seconds is a restatement of backlog depth, not a
measure of responsiveness, so it is not quoted as a latency.

Two smaller measured costs. **Heartbeating costs 5.8% of throughput on no-op jobs** (4,174 →
3,930 jobs/s, non-overlapping ranges) — a fixed per-job thread cost that matters only when jobs
are trivially short, which is exactly when leases are least likely to expire. On a 300 ms
handler, throughput is unchanged and the database cost is 105 extra commits per second, about
1% of this machine's commit ceiling ([cost of heartbeating](bench/reports/README.md#cost-of-heartbeating)).
And the benchmark's own worker was checked against the product one: 4,185 vs 4,184 jobs/s, a
0.0% difference ([instrument check](bench/reports/README.md#is-the-measuring-instrument-honest)).

### Chaos: the duplicate-execution window, demonstrated

The harness runs real worker subprocesses, `SIGKILL`s them at seeded points inside and outside
the ack window, runs a reaper on a timer, and restarts them. **Every assertion is computed from
rows the handler itself wrote** (`chaos_effects`, `chaos_executions`) plus the jobs table, never
from the harness's memory of what it did. Seed 42, 200 jobs, 6 workers, 30% kill probability
([`chaos/reports/seed-42.txt`](chaos/reports/seed-42.txt), raw run in
[`seed-42.json`](chaos/reports/seed-42.json)):

| | naive handler | idempotent handler |
|---|---|---|
| Jobs enqueued | 200 | 200 |
| Succeeded / dead | 193 / 7 (7 expected to fail) | 193 / 7 (7 expected to fail) |
| Not terminal | 0 | 0 |
| Executions (reached effect phase) | 355 (241) | 319 (229) |
| SIGKILLs / worker restarts | 111 / 111 | 78 / 78 |
| Leases reaped | 112 | 79 |
| **Duplicate executions** | **48** (39 jobs) | **36** (29 jobs) |
| **Duplicate effects** | **48** (39 jobs) | **0** |
| Result | OK | OK |

Read those last two rows together, because the second column is the whole point. The idempotent
handler's effect phase demonstrably ran twice or more for **29 jobs** — the failure window was
genuinely exercised, not merely avoided — and it still produced **zero** duplicate effect rows.
The only difference between the two handlers is that the idempotent one inserts a unique key
derived from the job id *in the same transaction as its effect*. That single change absorbs
every duplicate the queue delivers.

The naive run is not a failure; it is the control. A naive handler that showed zero duplicates
would mean the window was never hit, and the harness fails the run on that basis rather than
reporting a false pass.

### Three sabotage runs: the harness fails when it should

A test suite that only ever passes proves nothing about its own sensitivity. Each of these
deliberately broke one mechanism, confirmed the harness caught it, and restored the source.

| Sabotage | Mechanism broken | Expected | Result | Report |
|---|---|---|---|---|
| Ack before handler | Worker acks *before* running the handler, making delivery at-most-once | `[lost]` | **FAILED, 10 violations** — 8 jobs `succeeded` with zero effect rows | [`sabotage-ack-before-handler.txt`](chaos/reports/sabotage-ack-before-handler.txt) |
| Key guard removed | The idempotent handler's unique-key check disabled (`if idempotent` → `if False`) | `[duplicate]` | **FAILED, 9 violations** — 11 duplicate effects across 9 jobs, from an *idempotent* run | [`sabotage-no-key-guard.txt`](chaos/reports/sabotage-no-key-guard.txt) |
| Reaper disabled | The reaper never requeues expired leases | `[not_terminal]` | **FAILED, 19 violations** — 18 jobs stranded in `running`, 0 leases reaped | [`sabotage-reaper-disabled.txt`](chaos/reports/sabotage-reaper-disabled.txt) |

Each report contains the full interleaving for every offending job — attempt starts, effect
commits, SIGKILLs, reaps and acks on a single timeline — so the failure can be read rather than
inferred. The first run is worth dwelling on: acking before the handler produced 60/60
`succeeded` jobs and a clean-looking summary, while 8 of them had never performed their side
effect at all. That is precisely the shape of bug a queue that reports its own status cannot
catch, and it is why every assertion is computed from handler-written rows instead.

## Trade-offs

### Visibility timeouts vs holding row locks for the duration of a job

The alternative to a lease is the obvious one: open a transaction, `SELECT ... FOR UPDATE` the
job, run the handler while the transaction is still open, and commit at the end. The row lock
becomes the exclusion mechanism, and it is strictly stronger than a deadline — it cannot expire
early, cannot be extended, and needs no reaper; if the handler's writes go to the same database
they commit atomically with the status change, eliminating the duplicate window entirely. What
it costs is that job duration becomes a database resource. Every in-flight job pins a connection
and a backend for its full wall-clock duration, so concurrency is capped by `max_connections` —
100 on this machine — with each of those backends sitting `idle in transaction`. Worse, a long
transaction holds back the xmin horizon: a job that takes ten minutes prevents vacuum from
cleaning dead tuples in *every* table for those ten minutes, and on a busy system that bloat is
not theoretical. The failure modes are also less uniform than they look. A worker that dies
cleanly drops its TCP connection and releases the lock promptly, which is fine; a worker that
*hangs* — a wedged syscall, a network partition, a stopped container — holds it indefinitely
with no deadline to rescue it, and the only remedy is a blunt server-side
`idle_in_transaction_session_timeout`.

The lease decouples exclusion from the connection. The claim commits immediately, the connection
returns to normal use, and ownership becomes a timestamp any participant can evaluate. Dead and
hung workers collapse into the same case handled by the same clock, and in-flight jobs stop being
bounded by database connections. The price is paid in three places: the duplicate-execution
window that the rest of this README is largely about; the reaper as an extra moving part that
must actually be running for the system to make progress, which sabotage run three makes vivid —
18 jobs stranded in `running` forever; and the heartbeat plus fencing token needed for jobs that
outlive their own lease. That is three mechanisms bought to replace one lock. The trade is worth
it when handlers touch the outside world — an HTTP call, an email, an upload — because there the
atomicity the lock would have bought you was never available anyway. If every handler only ever
writes to the same Postgres, the lock design is simpler and stronger, and this queue is the wrong
shape for the job.

### What exactly-once would actually cost

Exactly-once *delivery* is not available at any price; it is the two-generals problem wearing a
hat. What people want is exactly-once *effects*, and the direct mechanism is to commit the
handler's writes in the same transaction as the ack: `BEGIN; <handler's writes>; UPDATE jobs SET
status='succeeded' ...; COMMIT`. There is then no window, because there is no moment at which
the effects exist and the ack does not. The price comes in four parts and they compound. The
handler's side effects must all live in the same PostgreSQL database as the queue — no HTTP
call, no email, no object store, nothing that cannot be rolled back. Either the transaction
stays open for the handler's full duration, which lands you back in the row-lock design and all
of its costs above, or the handler buffers every write in memory and flushes at the end, which
means it cannot read its own writes, cannot stream, and is bounded by RAM. The handler may no
longer commit anything itself, a real and easily-violated constraint that the queue cannot
enforce. And handlers must run in-process, foreclosing another language or another machine. The
cost is not performance; it is that the set of programs you are allowed to write shrinks
dramatically.

The indirect mechanism is to push idempotency to the effect site, which is what the chaos
harness demonstrates: the handler writes a unique key derived from the job id in the same
transaction as its effect, so a second execution hits the unique violation and skips. It is far
cheaper and it works — 36 duplicate executions, 0 duplicate effects — but the price is that it
is not the queue's guarantee, it is a discipline every handler author must implement correctly,
forever. Sabotage run two removed exactly one `if` from that handler and produced 11 duplicate
effects immediately. It also needs a dedup table that grows without bound and must eventually be
pruned, and pruning quietly reintroduces the window: delete a key before some long-stalled
zombie attempt finally arrives, and the duplicate lands. For the cases people most often care
about — charging a card, sending an email — none of it helps unless the *remote* system offers
an idempotency key of its own, because no amount of queue engineering makes a non-idempotent
remote API safe to call twice. So this queue does not offer exactly-once, and the reason is not
that it would be slow but that every mechanism delivering it either forbids the effects people
actually want or relocates the obligation onto the handler. What it offers instead is narrower:
the window is named and bounded, the fencing token guarantees the queue's own bookkeeping never
corrupts however the duplicates land, and the harness proves handler-level idempotency is
sufficient to close it.

### Why `attempts` is the fencing token rather than a separate lease ID

The textbook fencing token is a dedicated `lease_id UUID`, regenerated on every claim, with
`ack` and `nack` matching on `WHERE id = %s AND lease_id = %s`. Conveyor reuses `attempts`, the
integer already incremented in the claim's `UPDATE`. It is unique per claim by construction —
nothing else increments it — so as a fence it is exactly as sound as a UUID: the only question a
fence must answer is "is this writer's claim still the current one", and an integer that only
moves forward answers it completely. Reusing it costs zero extra bytes per row, zero extra writes
per claim, and zero risk of two pieces of lease state drifting apart; on a system whose measured
bottleneck is WAL write volume, not adding a column written on every claim is a real if modest
saving, and one fewer invariant to maintain is a larger one. The reuse also buys something a
UUID could not: ordering. Because `attempts` is monotonic it serves simultaneously as the fence
*and* as the retry budget (`attempts >= max_attempts` is what sends a job to `dead`), so one
column answers two questions that would otherwise need two. Monotonicity is also what makes
stale tokens *permanently* stale, which is why `deadletter replay` deliberately does not reset
the counter — a zombie still holding attempt 3 from before the job died would, against a reset
counter, find its token matching a genuinely fresh attempt 3 and mark work it never did as
succeeded.

The cost is conceptual rather than mechanical: one column now means two things, and the two
meanings diverge. After a replay, `attempts` no longer reads as "how many times this failed" but
as "how many claims have ever occurred", a different and less useful number, and that leak is
visible in the API through `--extra-attempts`. A separate `lease_id` would keep the two concepts
apart and let the attempt counter be reset freely and safely, which is a genuine advantage and
the honest argument for the conventional design. The judgement here is that a queue with a
five-state row and one table is better served by fewer columns carrying more meaning, and that
the divergence only surfaces on the dead-letter replay path — rare, operator-driven, and already
documented at the point where it bites.

## The delivery guarantee, precisely

**Every job is executed at least once. No job is silently dropped. A job may be executed more
than once, and two executions of the same job may overlap in time.**

The window that makes this at-least-once rather than exactly-once is the interval **between the
commit of the handler's side effects and the commit of the ack**:

```
        claim commits          effects committed         ack commits
             │                        │                       │
    ─────────┼────────────────────────┼───────────────────────┼─────────►
             │    handler running, outside any transaction    │
             │                        │                       │
             │                        ├─────── THE WINDOW ────┤
             │                                                │
             └──── lease held: visibility_deadline ───────────┘
```

Two events land a job in that window:

1. **The worker dies inside it.** The side effects happened; the ack never committed. The lease
   expires, the reaper requeues the job, and another worker runs it again.
2. **The lease expires while the handler is still running.** The reaper requeues the job and a
   second worker claims it *while the first is still executing*. This is the case where two
   executions overlap in time.

Heartbeating narrows case 2 to "the worker stalled for longer than its lease": `extend_lease`
pushes the deadline forward every third of the visibility timeout while the handler runs. It
does nothing for case 1, so delivery stays at-least-once. Without heartbeating there is a
sharper failure — a handler that *always* outlives its lease is reaped on every attempt, every
ack is fenced out, and the job reaches `dead` with `last_error='visibility deadline expired'`
having done its work every single time. The chaos harness found that before the heartbeat
existed; [`tests/test_heartbeat.py`](tests/test_heartbeat.py) reproduces it with heartbeating off.

**What the fencing token does and does not protect.** `ack`, `nack` and `extend_lease` all match
on `attempts`, so a stale worker's write matches zero rows and is discarded. The queue's
bookkeeping is therefore always consistent: a superseded attempt can never mark a job succeeded,
never revive a lease it has lost, and never overwrite a newer attempt's error. It does **not**
undo the duplicate side effects, which already happened in the outside world. Fencing protects
the queue, not your database.

**Enqueue idempotency is a separate guarantee, about a different thing.** A unique index on
`idempotency_key` means a second enqueue with the same key returns the existing job and creates
nothing — under concurrent inserts, and even after the job has succeeded, died or been
cancelled. That is a guarantee about *enqueueing*. The one job it produces can still execute
twice for the reasons above.

**Claims are exclusive.** Two workers can never hold the same job: the row lock inside the claim
transaction is what makes that true, not `SKIP LOCKED`, which only decides whether the loser
blocks or moves on.

## What the SDK does not do

[`conveyor_client`](conveyor_client) takes a connection string and exposes `enqueue`, `get` and
`cancel`, with connection pooling and full type annotations. It imports nothing from
`conveyor/` — [`tests_client/test_independence.py`](tests_client/test_independence.py) enforces
that with an AST scan and a subprocess `sys.modules` check — so the queue's internals stay free
to change. What it deliberately leaves out, and why:

1. **It does not consume jobs.** No `claim`, `ack`, `nack`, `extend_lease`, no worker loop.
   Consuming means owning the lease protocol — fencing tokens, heartbeat cadence, reaper
   interaction — which is exactly the part of the queue that has to stay free to change. A
   client that reimplemented `claim` would freeze the subtlest contract in the system from the
   outside.
2. **It does not dispatch.** The `handler` argument is a string it stores in a payload envelope,
   `{"handler": ..., "payload": ...}`. Nothing in this repository routes on it; `conveyor.worker`
   is still one handler per process and receives the whole envelope. Unwrap it at the boundary as
   shown in the quickstart — [`tests/test_sdk_interop.py`](tests/test_sdk_interop.py) exercises
   exactly that against a real worker.
3. **It does not expose the lease.** `visibility_deadline`, `claimed_by` and `claimed_at` are not
   on `Job` and are not even selected. They are worker and reaper bookkeeping, and a producer
   reading them would couple to the machinery most likely to move.
4. **It does not manage the schema.** No `CREATE TABLE`, no migrations, no version handshake. It
   is a client of a database someone else owns, so when the two diverge it raises
   `SchemaMismatch` with the fix in the message rather than quietly altering your table.
5. **It does not cancel running work.** Only `queued → cancelled`. Handlers run outside any
   transaction with no interrupt channel, so once a job is claimed it runs to completion; a
   cancel that loses that race raises `NotCancellable` and changes nothing.
6. **It does not retry, and it does not hide `psycopg` errors.** The pool checks connections and
   recycles dead ones, but an `OperationalError` propagates as itself. A silently swallowed
   enqueue is worse than a raised one, and the retry policy belongs to the caller.
7. **It does not tell you whether an idempotent enqueue deduplicated.** `enqueue` returns the
   `Job` either way, which keeps `Job` a faithful view of the row. Compare `job.created_at`
   against the time of the call if you need to know.
8. **No bulk enqueue, no listing, no async.** One row per call, three verbs, synchronous.
   Dead letters and replay stay on the `conveyor.deadletter` CLI: they are operator tools that
   read the whole table, not something an application producer should reach for.

## Reproducing

```sh
pytest -q                                            # 91 tests, needs the database

python -m chaos.run --mode both --seed 42            # exit 1 on any violation
python -m chaos.run --mode naive --kill-timing after_effect --kill-probability 0.6

python -m bench.load all --workers 1,2,4,8,16 --reps 3 --out bench/reports
python -m bench.report bench/reports/results-<stamp>.json > bench/reports/README.md
```

The chaos harness fails a `naive` run that shows *no* duplicates, on the grounds that the window
was never exercised and an idempotent pass would prove nothing. Heartbeating is off there by
default so overrun jobs really are reaped mid-run; `--heartbeat` turns it on. The load harness
truncates the jobs table and refuses to start if the queue already holds queued or running work.

## Layout

- [`db/schema.sql`](db/schema.sql) — the `jobs` table and its three indexes, with why each exists.
- [`db/migrations/`](db/migrations) — changes for databases already initialised.
- [`conveyor/queue.py`](conveyor/queue.py) — `enqueue`, `claim`, `ack`, `nack`, `extend_lease`, `backoff_delay`.
- [`conveyor/worker.py`](conveyor/worker.py) — the `Worker` loop and CLI. SIGTERM finishes the job in flight, touches no other job, exits 0.
- [`conveyor/reaper.py`](conveyor/reaper.py) — returns expired leases to the queue, or marks them dead if the expired attempt was the last one.
- [`conveyor/deadletter.py`](conveyor/deadletter.py) — inspect and replay jobs that exhausted their attempts.
- [`conveyor_client/`](conveyor_client) — the client SDK. `_sql.py` holds the entire coupling to the schema in one file.
- [`chaos/`](chaos) — kill-and-restart harness; assertions computed from handler-written rows.
- [`bench/`](bench) — load harness, wait-event sampler, and report generator.
- [`tests/`](tests) — real Postgres, real threads. `test_sdk_interop.py` is where the SDK and the worker meet.
- [`tests_client/`](tests_client) — the SDK's own suite, with no `conveyor` import anywhere in it.
