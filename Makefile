.PHONY: all install test clean build publish format format-check lint lint-fix check coverage help

all: format test

install:
	uv sync --all-extras

build:
	uv build

publish: build
	uv publish

clean:
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	rm -rf dist/
	rm -rf build/
	rm -rf *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

test:
	uv run --extra dev pytest -q

coverage:
	uv run --extra dev pytest tests/ --cov=pretty_cfn --cov-report=term-missing

lint:
	uv run --extra dev ruff check pretty_cfn tests examples

lint-fix:
	uv run --extra dev ruff check --fix pretty_cfn tests examples

format:
	uv run --extra dev ruff format pretty_cfn tests examples

format-check:
	uv run --extra dev ruff format --check pretty_cfn tests examples
check: lint format-check test

help:
	@echo "Available targets:"
	@echo "  install         - Install dependencies"
	@echo "  test            - Run tests"
	@echo "  coverage        - Run tests with coverage report"
	@echo "  lint            - Run linting checks with ruff"
	@echo "  lint-fix        - Auto-fix linting issues with ruff"
	@echo "  format          - Format code with ruff"
	@echo "  format-check    - Check code formatting with ruff"
	@echo "  check           - Run lint, format-check, and tests"
	@echo "  build           - Build the package"
	@echo "  publish         - Build and publish to PyPI"
	@echo "  clean           - Clean build artifacts"
	@echo ""
	@echo "Example-specific targets now live in examples/Makefile (run 'make -C examples help')."
