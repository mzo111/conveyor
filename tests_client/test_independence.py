"""The SDK must not import the queue's internals.

This is the constraint the whole package exists to satisfy, so it is enforced
here rather than left as a convention someone has to remember during review.

Two checks, because either alone has a hole. The static scan catches an import
written anywhere in the package, including inside a function that no test
happens to call. The subprocess check catches an import pulled in indirectly by
a dependency at import time. Together they cover both.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

import conveyor_client

PACKAGE = Path(conveyor_client.__file__).parent
MODULES = sorted(PACKAGE.glob("*.py"))


def test_the_package_has_modules_to_scan() -> None:
    """Guard against the scan below passing because it found nothing."""
    assert len(MODULES) >= 5


@pytest.mark.parametrize("module", MODULES, ids=lambda p: p.name)
def test_no_module_imports_conveyor(module: Path) -> None:
    tree = ast.parse(module.read_text(), filename=str(module))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported += [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            imported.append(node.module)

    offenders = [name for name in imported if name == "conveyor" or name.startswith("conveyor.")]
    assert not offenders, f"{module.name} imports the queue internals: {offenders}"


def test_importing_the_sdk_does_not_load_conveyor() -> None:
    """Fresh interpreter: nothing named `conveyor` should end up in sys.modules.

    Run out of the repository root, where `conveyor/` is importable, so this
    fails if the import ever creeps in.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import conveyor_client, sys; "
            "print([m for m in sys.modules if m == 'conveyor' or m.startswith('conveyor.')])",
        ],
        cwd=PACKAGE.parent,
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "[]", result.stdout
