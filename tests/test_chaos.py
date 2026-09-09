"""Run the chaos harness at a small size on every push.

These are the tests that can falsify the project's central claim. Nothing here
may be relaxed to make it pass; a failure prints the interleaving that caused it.
"""

from __future__ import annotations

import pytest

from chaos.harness import ChaosConfig, run

SMALL = dict(workers=4, jobs=60, seed=1234, duration=90.0, kill_probability=0.35)


@pytest.mark.parametrize("mode", ["naive", "idempotent"])
def test_chaos(conn, dsn, mode):
    result = run(ChaosConfig(mode=mode, dsn=dsn, **SMALL))
    print(result.report())
    assert result.ok, result.report()
    s = result.stats
    assert s["kills"] > 0
    assert s["succeeded"] + s["dead"] == s["enqueued"]
    if mode == "naive":
        assert s["duplicate_effects"] > 0, "window not exercised; see report"
    else:
        assert s["duplicate_effects"] == 0
        assert s["duplicate_executions"] > 0, "window not exercised; see report"
