"""Keep the load test runnable. Not a performance assertion.

A real sweep takes minutes, so CI runs one tiny batch and checks the harness
still measures what it claims to: every job completes, rates are positive, and
the percentiles and samplers produce sane values.
"""

from __future__ import annotations

import math

from bench.load import BenchConfig, percentiles, run_once


def test_percentiles_are_ordered_and_nearest_rank():
    values = [float(i) for i in range(1, 101)]
    p = percentiles(values)
    assert p["p50"] <= p["p95"] <= p["p99"]
    assert (p["p50"], p["p95"], p["p99"]) == (50.0, 95.0, 99.0)
    empty = percentiles([])
    assert all(math.isnan(v) for v in empty.values())


def test_bench_run_completes_and_measures(conn, dsn):
    cfg = BenchConfig(
        workers=2, jobs_per_worker=100, min_jobs=200, dsn=dsn, timeout=90.0, label="smoke"
    )
    result = run_once(cfg)

    assert result.completed == result.jobs == 200, result.notes
    assert result.interior_throughput > 0
    assert result.wallclock_throughput > 0
    assert result.worker_stats["processed"] == 200
    assert result.worker_stats["ack_rejected"] == 0

    for lat in (result.claim_latency_ms, result.ack_latency_ms, result.e2e_latency_ms):
        assert lat["p50"] <= lat["p95"] <= lat["p99"]
        assert lat["p50"] > 0

    assert result.waits["samples"] > 0, "wait-event sampler collected nothing"
    assert result.cpu["host_cpu_busy_fraction"] is not None
    # Two commits per job is the design: the claim and the ack.
    assert result.db["commits_per_job"] >= 2.0
