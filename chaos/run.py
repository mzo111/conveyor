"""CLI for the chaos harness.

    python -m chaos.run --mode both --seed 42
    python -m chaos.run --mode naive --kill-timing after_effect --kill-probability 0.6

Exit status is 1 on the first mode that violates an assertion; the report
printed for it includes the exact interleaving for every offending job.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import fields
from pathlib import Path

from chaos.harness import KILL_TIMINGS, MODES, ChaosConfig, run


def build_parser() -> argparse.ArgumentParser:
    defaults = ChaosConfig()
    p = argparse.ArgumentParser(description="Kill real workers at real points; check the ledger.")
    p.add_argument("--mode", choices=(*MODES, "both"), default="both")
    p.add_argument("--workers", type=int, default=defaults.workers)
    p.add_argument("--jobs", type=int, default=defaults.jobs)
    p.add_argument(
        "--kill-probability",
        type=float,
        default=defaults.kill_probability,
        help="per handler execution",
    )
    p.add_argument("--kill-timing", choices=KILL_TIMINGS, default=defaults.kill_timing)
    p.add_argument(
        "--duration",
        type=float,
        default=defaults.duration,
        help="max wall-clock seconds; jobs not terminal by then are a failure",
    )
    p.add_argument("--seed", type=int, default=None, help="default: random, printed")
    p.add_argument("--visibility-timeout", type=float, default=defaults.visibility_timeout)
    p.add_argument("--reaper-interval", type=float, default=defaults.reaper_interval)
    p.add_argument("--max-sleep", type=float, default=defaults.max_sleep)
    p.add_argument("--fail-probability", type=float, default=defaults.fail_probability)
    p.add_argument("--overrun-probability", type=float, default=defaults.overrun_probability)
    p.add_argument("--max-attempts", type=int, default=defaults.max_attempts)
    p.add_argument("--restart-delay", type=float, default=defaults.restart_delay)
    p.add_argument(
        "--heartbeat",
        action="store_true",
        help="workers extend their lease while running (default off, so overrun jobs "
        "are reaped mid-run and the concurrent-execution window is exercised)",
    )
    p.add_argument("--log-dir", default=None, help="worker stderr logs (default: temp dir)")
    p.add_argument("--dsn", default=None, help="defaults to $DATABASE_URL")
    p.add_argument("--json", default=None, help="write the full result(s) to this file")
    p.add_argument("-v", "--verbose", action="store_true", help="log every kill and reap")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    names = {f.name for f in fields(ChaosConfig)} - {"mode"}
    base = {k: v for k, v in vars(args).items() if k in names}
    modes = ("naive", "idempotent") if args.mode == "both" else (args.mode,)

    results = []
    for mode in modes:
        result = run(ChaosConfig(mode=mode, **base))
        results.append(result)
        print(result.report())
        print()
        if not result.ok:
            break
    if args.json:
        Path(args.json).write_text(
            json.dumps([r.to_dict() for r in results], indent=2, default=str)
        )
    return 0 if all(r.ok for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
