"""Ensure the example templates remain valid per cfn-lint."""

from __future__ import annotations

from pathlib import Path

import pytest
from cfnlint.api import ManualArgs, lint_file


EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"
TEMPLATES = sorted(path for path in EXAMPLES_DIR.glob("*/output.yaml") if path.is_file())

if not TEMPLATES:
    pytest.skip("No example templates available for cfn-lint", allow_module_level=True)

IGNORED_RULES = ("W2001",)


@pytest.mark.parametrize("template_path", TEMPLATES, ids=lambda p: p.parent.name)
def test_examples_pass_cfn_lint(template_path: Path) -> None:
    """Run cfn-lint against each generated example template."""

    config = ManualArgs(ignore_checks=list(IGNORED_RULES))
    matches = lint_file(template_path, config=config)
    assert matches == [], [f"[{m.rule.id}] {m.message}" for m in matches]
