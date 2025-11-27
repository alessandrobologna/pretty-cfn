# Repository Guidelines

Pretty CFN formats CloudFormation, SAM, and CDK output for CLI users and MCP clients. Use these guardrails so every change fits the pipeline.

## Project Structure & Module Organization
- `pretty_cfn/cli.py` + `main.py` only parse flags; real work lives in `service.py`, `formatter.py`, and the modular `samifier/` helpers.
- Sample stacks and golden outputs stay in `aws-cdk-examples/`, `examples/`, and curated `Sam*/` directories—regenerate via the CLI.
- `tests/` mirrors the runtime layout; keep fixtures beside the test that consumes them.
- MCP helpers live under `pretty_cfn/agents/` (deterministic refactor workflows). Add coverage alongside any new helpers.

## Build, Test, and Development Commands
- `make install` bootstraps deps via `uv sync`; run `make format` and `make lint` before touching code.
- `uv run --extra dev pytest -q` (or `make test`) covers unit suites; finish with `make coverage` or the stacked `make check`.
- Exercise the CLI (`pretty-cfn format template.yaml`, `pretty-cfn refactor --stack-name chat-app --target sam-app --output SamCdkChatApp --overwrite`) to keep docs and staging paths honest.

## Coding Style & Naming Conventions
- Python 3.10+, four-space indents, Ruff line length 100; `ruff format` is the only formatter.
- Mirror CLI flags or resource names in helper identifiers and favor short docstrings or Click `help=` text.
- Entry points stay thin—push logic into formatter/service/samifier modules and keep shared utilities in `shared.py`.

## Testing Guidelines
- Pytest picks up `test_*.py`; pair each logic change with a module test and refresh integrations (notably `tests/test_examples_cfn_lint.py`) when YAML output shifts.
- `tests/test_samifier.py` should cover WebSocket folding, IAM hoisting, and asset staging; re-use sample stacks or golden files instead of inline blobs.
- End feature branches with `make coverage` and document cfn-lint suppressions directly in the failing test or fixture.

## Commit & Pull Request Guidelines
- Keep commit subjects short and imperative (“Fold WebSocket API into SAM”). Bodies should cite affected stacks, include `pretty-cfn --diff` snippets, and call out new flags.
- Each PR should state `make check` (or `sam validate`/`sam build` for sam-app work), link issues or stack IDs, and share before/after template excerpts when behavior changes.

## Security & Configuration Tips
- `pretty-cfn --stack-name` pulls live templates with your AWS credentials; favor sandbox profiles, strip secrets from staged files, and record any `AWS_PROFILE` / `AWS_REGION` prerequisites in docs or PRs.
- Asset staging downloads S3 bundles into local `src/` folders so `sam build` can run offline—keep them sample-safe and never commit customer data.
