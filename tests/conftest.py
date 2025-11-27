"""Shared pytest fixtures."""

import pytest


@pytest.fixture(autouse=True)
def disable_cli_lint(monkeypatch):
    """Stub out cfn-lint during most CLI tests; individual tests can override."""

    from pretty_cfn import service

    def _no_lint(content: str, template_name: str):  # pragma: no cover - trivial stub
        return [], []

    monkeypatch.setattr(service, "lint_template", _no_lint)


@pytest.fixture
def anyio_backend():
    """Force asyncio backend for anyio tests."""
    return "asyncio"
