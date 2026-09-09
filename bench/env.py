"""Collect the hardware, Postgres and toolchain facts behind a benchmark run.

Everything here is read from the machine at run time. Nothing is hand-typed
into a report, so the numbers and the environment that produced them cannot
drift apart.
"""

from __future__ import annotations

import json
import platform
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import DictRow

ROOT = Path(__file__).resolve().parent.parent


def _run(cmd: list[str]) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=30).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def _cpu_model() -> str:
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown"


def _mem_total_gib() -> float | None:
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                return round(int(line.split()[1]) / 1024 / 1024, 1)
    except OSError:
        pass
    return None


def host_facts() -> dict[str, Any]:
    kernel = platform.release()
    disk = shutil.disk_usage("/")
    return {
        "cpu_model": _cpu_model(),
        "cpu_logical_cores": len(_cpu_list()),
        "memory_gib": _mem_total_gib(),
        "kernel": kernel,
        "is_wsl": "microsoft" in kernel.lower(),
        "platform": platform.platform(),
        "root_disk_total_gib": round(disk.total / 1024**3, 1),
        "root_disk_free_gib": round(disk.free / 1024**3, 1),
        "docker_version": _run(["docker", "--version"]),
        "docker_storage_driver": _run(["docker", "info", "--format", "{{.Driver}}"]),
        "docker_host_os": _run(["docker", "info", "--format", "{{.OperatingSystem}}"]),
        "python": sys.version.split()[0],
        "psycopg": psycopg.__version__,
    }


def _cpu_list() -> list[str]:
    try:
        return [ln for ln in Path("/proc/stat").read_text().splitlines() if re.match(r"cpu\d", ln)]
    except OSError:
        return []


def postgres_facts(conn: psycopg.Connection[DictRow]) -> dict[str, Any]:
    version = conn.execute("SELECT version() AS v").fetchone()["v"]
    # Everything the server is not running at its compiled-in default: this is
    # the honest way to show "stock config" without pasting 300 settings.
    non_default = conn.execute(
        """
        SELECT name, setting, unit, source
        FROM pg_settings
        WHERE source NOT IN ('default', 'override')
          AND name NOT IN ('application_name', 'client_encoding')
        ORDER BY name
        """
    ).fetchall()
    interesting = conn.execute(
        """
        SELECT name, setting, unit
        FROM pg_settings
        WHERE name IN ('max_connections', 'shared_buffers', 'work_mem', 'effective_cache_size',
                       'synchronous_commit', 'fsync', 'full_page_writes', 'wal_buffers',
                       'wal_level', 'max_wal_size', 'checkpoint_timeout', 'commit_delay',
                       'autovacuum', 'track_io_timing', 'random_page_cost')
        ORDER BY name
        """
    ).fetchall()
    conn.commit()
    return {
        "version": version,
        "non_default_settings": [dict(r) for r in non_default],
        "key_settings": [dict(r) for r in interesting],
    }


def compose_service() -> str:
    path = ROOT / "docker-compose.yml"
    return path.read_text() if path.exists() else ""


def collect(conn: psycopg.Connection[DictRow]) -> dict[str, Any]:
    return {"host": host_facts(), "postgres": postgres_facts(conn), "compose": compose_service()}


def render_markdown(env: dict[str, Any]) -> str:
    h, p = env["host"], env["postgres"]

    def fmt(row: dict[str, Any]) -> str:
        unit = f" {row['unit']}" if row.get("unit") else ""
        return f"| `{row['name']}` | {row['setting']}{unit} |"

    lines = [
        "# Benchmark environment",
        "",
        "Collected automatically by `bench/env.py` during the run. Not hand-written.",
        "",
        "## Host",
        "",
        "| | |",
        "|---|---|",
        f"| CPU | {h['cpu_model']} |",
        f"| Logical cores | {h['cpu_logical_cores']} |",
        f"| Memory | {h['memory_gib']} GiB |",
        f"| Kernel | {h['kernel']} |",
        f"| Platform | {h['platform']} |",
        f"| Docker | {h['docker_version']} |",
        f"| Docker storage driver | {h['docker_storage_driver']} |",
        f"| Root disk | {h['root_disk_total_gib']} GiB total, {h['root_disk_free_gib']} GiB free |",
        f"| Python | {h['python']} |",
        f"| psycopg | {h['psycopg']} |",
        "",
    ]
    if h["is_wsl"]:
        lines += [
            "This is WSL2. The Postgres container writes through the WSL2 virtual disk, so "
            "fsync latency is not the same as bare metal on the same SSD, and the workers "
            "share these cores with the database.",
            "",
        ]
    lines += [
        "## PostgreSQL",
        "",
        f"```\n{p['version']}\n```",
        "",
        "Settings that matter for this workload:",
        "",
        "| Setting | Value |",
        "|---|---|",
        *[fmt(r) for r in p["key_settings"]],
        "",
        "Every setting not at its compiled-in default (that is, everything the image or "
        "Compose file sets):",
        "",
        "| Setting | Value |",
        "|---|---|",
        *[fmt(r) for r in p["non_default_settings"]],
        "",
        "## Compose service",
        "",
        f"```yaml\n{env['compose'].strip()}\n```",
        "",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    from conveyor.db import connect

    with connect() as c:
        print(json.dumps(collect(c), indent=2, default=str))
