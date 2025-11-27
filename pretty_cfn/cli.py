"""Command-line interface for pretty-cfn."""

from __future__ import annotations

import difflib
import json
import shutil
import sys
from pathlib import Path
from typing import Callable, Optional, Sequence

import click

from .samifier import SamAssetStager
from .service import (
    LintIssue,
    TemplateProcessingError,
    TemplateProcessingOptions,
    TemplateProcessingResult,
    TemplateSource,
    fetch_stack_template as _service_fetch_stack_template,
    process_template,
)


CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}
REFACTOR_TARGETS = ("clean-cfn", "sam-app", "report-only")
STRATEGY_CHOICES = ("auto", "cdk", "serverless", "sam")
SAM_TEMPLATE_NAME = "template.yaml"
SAM_ASSET_SUBDIR = "src"


def _formatter_options(command: Callable) -> Callable:
    options = [
        click.option(
            "--input",
            "input_path",
            type=click.Path(path_type=Path),
            help="Path to a local template. When omitted, stdin is used.",
        ),
        click.option(
            "-o",
            "--output",
            type=click.Path(path_type=Path),
            help="Write output to this path instead of stdout.",
        ),
        click.option(
            "--overwrite",
            is_flag=True,
            help="Replace the input file in place (mutually exclusive with --output/--diff/--check).",
        ),
        click.option(
            "-c",
            "--column",
            type=int,
            default=40,
            show_default=True,
            help="Alignment column for values.",
        ),
        click.option(
            "--flow-style",
            type=click.Choice(["block", "compact"], case_sensitive=False),
            default="block",
            show_default=True,
            help="Controls inline {} and [] formatting for small maps and lists.",
        ),
        click.option("--check", is_flag=True, help="Exit 1 if formatting would change the file."),
        click.option("--diff", is_flag=True, help="Show a unified diff vs the formatted output."),
        click.option(
            "--diff-exit-code",
            is_flag=True,
            help="With --diff, exit 1 when differences are present.",
        ),
        click.option("--plain", is_flag=True, help="Disable syntax highlighting on stdout."),
        click.option(
            "--stack-name",
            type=str,
            help="Fetch template from a CloudFormation stack instead of a local file.",
        ),
        click.option("--lint", is_flag=True, help="Print cfn-lint warnings (errors always print)."),
        click.option(
            "--ignore-errors",
            is_flag=True,
            help="Proceed even if cfn-lint errors are detected.",
        ),
    ]

    for option in reversed(options):
        command = option(command)
    return command


@click.command(name="format", context_settings=CONTEXT_SETTINGS)
@_formatter_options
def format_command(
    input_path: Optional[Path],
    output: Optional[Path],
    overwrite: bool,
    column: int,
    flow_style: str,
    check: bool,
    diff: bool,
    diff_exit_code: bool,
    plain: bool,
    stack_name: Optional[str],
    lint: bool,
    ignore_errors: bool,
) -> None:
    """Format CloudFormation templates from files, stdin, or stacks."""

    _validate_formatter_args(input_path, output, overwrite, stack_name, check, diff)
    source, override = _prepare_source(input_path, stack_name, check, diff)
    relative_base = _relative_base(input_path, output)
    options = TemplateProcessingOptions(
        column=column,
        flow_style=flow_style,
        samify_relative_base=relative_base,
    )
    result = _run_pipeline(source, options, override)
    exit_code = _finalize_text_output(
        result,
        output,
        overwrite,
        input_path,
        check,
        diff,
        diff_exit_code,
        plain,
        lint,
        ignore_errors,
    )
    if exit_code:
        sys.exit(exit_code)


@click.command(name="refactor", context_settings=CONTEXT_SETTINGS)
@_formatter_options
@click.option(
    "--target",
    type=click.Choice(REFACTOR_TARGETS, case_sensitive=False),
    default="clean-cfn",
    show_default=True,
    help="Desired output: clean CFN, SAM app, or report.",
)
@click.option(
    "--strategy",
    type=click.Choice(STRATEGY_CHOICES, case_sensitive=False),
    default="auto",
    show_default=True,
    help="Optional override for source detection (unused for now).",
)
@click.option(
    "--plan",
    "plan_path",
    type=click.Path(path_type=Path),
    help="Write a JSON manifest describing the performed refactor.",
)
@click.option(
    "--prefer-external",
    is_flag=True,
    help="When targeting sam-app, spill inline assets (Lambda code, GraphQL schema, etc.) to files.",
)
def refactor_command(
    input_path: Optional[Path],
    output: Optional[Path],
    overwrite: bool,
    column: int,
    flow_style: str,
    check: bool,
    diff: bool,
    diff_exit_code: bool,
    plain: bool,
    stack_name: Optional[str],
    lint: bool,
    target: str,
    strategy: str,
    plan_path: Optional[Path],
    prefer_external: bool,
    ignore_errors: bool,
) -> None:
    """Perform structural rewrites such as CDK cleaning or SAM scaffolding."""

    normalized_target = target.lower()
    if normalized_target == "sam-app":
        _run_sam_refactor(
            input_path=input_path,
            output=output,
            overwrite=overwrite,
            column=column,
            flow_style=flow_style,
            stack_name=stack_name,
            lint=lint,
            strategy=strategy,
            plan_path=plan_path,
            prefer_external=prefer_external,
            ignore_errors=ignore_errors,
        )
        return

    if normalized_target == "report-only":
        _run_report_refactor(
            input_path=input_path,
            output=output,
            column=column,
            flow_style=flow_style,
            stack_name=stack_name,
            lint=lint,
            strategy=strategy,
            plan_path=plan_path,
        )
        return

    # clean-cfn default
    _validate_formatter_args(input_path, output, overwrite, stack_name, check, diff)
    source, override = _prepare_source(input_path, stack_name, check, diff)
    relative_base = _relative_base(input_path, output)
    options = TemplateProcessingOptions(
        column=column,
        flow_style=flow_style,
        cdk_clean=True,
        cdk_rename=True,
        cdk_semantic_naming=True,
        cdk_keep_path_metadata=True,
        cdk_collision_strategy="numbered",
        samify_relative_base=relative_base,
    )
    result = _run_pipeline(source, options, override)
    exit_code = _finalize_text_output(
        result,
        output,
        overwrite,
        input_path,
        check,
        diff,
        diff_exit_code,
        plain,
        lint,
        ignore_errors,
    )
    if plan_path:
        _write_plan(plan_path, target="clean-cfn", strategy=strategy, result=result)
    if exit_code:
        sys.exit(exit_code)


@click.group(context_settings=CONTEXT_SETTINGS)
def cli() -> None:
    """Pretty CFN multi-command entry point."""


COMMAND_MAP = {"format": format_command, "refactor": refactor_command}
for name, command in COMMAND_MAP.items():
    cli.add_command(command, name=name)


def dispatch_cli(argv: Optional[Sequence[str]] = None) -> None:
    args = list(sys.argv[1:] if argv is None else argv)
    cli.main(args=args, prog_name="pretty-cfn")


main = format_command


# Helper utilities ---------------------------------------------------------------------------


def apply_syntax_highlighting(text: str) -> str:
    try:
        from pygments import highlight
        from pygments.lexers import YamlLexer
        from pygments.formatters import TerminalFormatter

        return highlight(text, YamlLexer(), TerminalFormatter())
    except ImportError:
        return text


def should_use_colors(disable_colors: bool, output: Optional[Path], is_tty: bool) -> bool:
    if disable_colors:
        return False
    return output is None and is_tty


def _validate_formatter_args(
    input_path: Optional[Path],
    output: Optional[Path],
    overwrite: bool,
    stack_name: Optional[str],
    check: bool,
    diff: bool,
) -> None:
    normalized_input = _normalize_input_path(input_path)
    if stack_name and normalized_input is not None:
        _bail("--stack-name cannot be combined with --input")
    if overwrite and normalized_input is None:
        _bail("--overwrite requires --input")
    if overwrite and output is not None:
        _bail("--overwrite cannot be used with --output")
    if overwrite and (diff or check):
        _bail("--overwrite cannot be used with --diff/--check")
    if check and normalized_input is None:
        _bail("--check requires --input")


def _normalize_input_path(value: Optional[Path]) -> Optional[Path]:
    if value is None:
        return None
    return None if str(value) == "-" else value


def _prepare_source(
    input_path: Optional[Path],
    stack_name: Optional[str],
    check: bool,
    diff: bool,
) -> tuple[TemplateSource, Optional[str]]:
    normalized_input = _normalize_input_path(input_path)
    if stack_name:
        inline = _service_fetch_stack_template(stack_name)
        source = TemplateSource(path=None, stack_name=None, inline_content=inline)
        return source, f"stack:{stack_name}"

    stdin_content: Optional[str] = None
    if normalized_input is None:
        if sys.stdin.isatty() and not check and not diff:
            _bail("No input provided. Use --input or pipe a template via stdin.")
        stdin_content = sys.stdin.read()
        if not stdin_content:
            _bail("stdin was empty")

    source = TemplateSource(
        path=normalized_input if stdin_content is None else None,
        stack_name=None,
        inline_content=stdin_content,
    )
    return source, None


def _relative_base(input_path: Optional[Path], output: Optional[Path]) -> Path:
    normalized_input = _normalize_input_path(input_path)
    if output is not None:
        try:
            return output.resolve().parent
        except OSError:
            return Path.cwd()
    if normalized_input is not None:
        try:
            return normalized_input.resolve().parent
        except OSError:
            return Path.cwd()
    return Path.cwd()


def _run_pipeline(
    source: TemplateSource,
    options: TemplateProcessingOptions,
    source_name_override: Optional[str],
    *,
    sam_asset_stager: Optional[SamAssetStager] = None,
) -> TemplateProcessingResult:
    try:
        result = process_template(source, options, sam_asset_stager=sam_asset_stager)
    except TemplateProcessingError as exc:
        _bail(str(exc))

    if source_name_override:
        result.source_name = source_name_override

    _emit_messages(result)
    return result


def _finalize_text_output(
    result: TemplateProcessingResult,
    output: Optional[Path],
    overwrite: bool,
    input_path: Optional[Path],
    check: bool,
    diff: bool,
    diff_exit_code: bool,
    plain: bool,
    lint_flag: bool,
    ignore_errors: bool,
) -> int:
    lint_failed = _print_lint_findings(result, lint_flag)
    _enforce_check(result, check)
    _enforce_diff(result, diff, diff_exit_code, output)

    target_output = output if output else (_normalize_input_path(input_path) if overwrite else None)

    if lint_failed and not ignore_errors and target_output is not None:
        click.echo(
            "Lint errors detected; --overwrite skipped to preserve the original file. "
            "Use --ignore-errors to write output anyway.",
            err=True,
        )
        target_output = None  # Write to stdout instead
    elif lint_failed:
        click.echo("Lint errors detected but ignored.", err=True)

    _write_output_text(result.formatted_content, target_output, plain)
    return 1 if (lint_failed and not ignore_errors) else 0


def _write_output_text(text: str, target: Optional[Path], plain: bool) -> None:
    normalized = text if text.endswith("\n") else text + "\n"
    if target is None:
        use_colors = should_use_colors(plain, None, sys.stdout.isatty())
        payload = apply_syntax_highlighting(normalized) if use_colors else normalized
        sys.stdout.write(payload)
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(normalized)
        click.echo(f"✓ Wrote formatted template to {target}", err=True)


def _print_lint_findings(result: TemplateProcessingResult, lint_flag: bool) -> bool:
    if result.lint_errors:
        for issue in result.lint_errors:
            click.echo(_format_lint_issue(issue), err=True)
    if lint_flag and result.lint_warnings:
        for issue in result.lint_warnings:
            click.echo(_format_lint_issue(issue), err=True)
    return bool(result.lint_errors)


def _enforce_check(result: TemplateProcessingResult, check: bool) -> None:
    if not check:
        return
    normalized_formatted = (
        result.formatted_content
        if result.formatted_content.endswith("\n")
        else result.formatted_content + "\n"
    )
    if result.original_content == normalized_formatted:
        click.echo(f"✓ {result.source_name} is already formatted")
        sys.exit(0)
    click.echo(f"✗ {result.source_name} needs formatting", err=True)
    sys.exit(1)


def _enforce_diff(
    result: TemplateProcessingResult,
    diff: bool,
    diff_exit_code: bool,
    output: Optional[Path],
) -> None:
    if not diff:
        return
    to_file = str(output) if output else "<stdout>"
    diff_lines = difflib.unified_diff(
        result.original_content.splitlines(keepends=True),
        result.formatted_content.splitlines(keepends=True),
        fromfile=result.source_name,
        tofile=to_file,
    )
    diff_output = "".join(diff_lines)
    click.echo(diff_output if diff_output else "No changes needed")
    if diff_output and diff_exit_code:
        sys.exit(1)
    sys.exit(0)


def _emit_messages(result: TemplateProcessingResult) -> None:
    for message in result.messages:
        color = "yellow" if message.level == "warning" else "blue"
        label = message.level.upper()
        click.echo(click.style(f"{label}: {message.text}", fg=color), err=True)


def _format_lint_issue(issue: LintIssue) -> str:
    return f"[{issue.rule_id}] {issue.message} ({issue.filename}:{issue.line}:{issue.column})"


def _bail(message: str) -> None:
    click.echo(f"Error: {message}", err=True)
    sys.exit(1)


# Refactor helpers --------------------------------------------------------------------------


def _run_sam_refactor(
    *,
    input_path: Optional[Path],
    output: Optional[Path],
    overwrite: bool,
    column: int,
    flow_style: str,
    stack_name: Optional[str],
    lint: bool,
    strategy: str,
    plan_path: Optional[Path],
    prefer_external: bool,
    ignore_errors: bool,
) -> None:
    project_dir = output
    if project_dir is None:
        _bail("--output is required when --target sam-app")
        return
    project_dir = project_dir.resolve()
    if project_dir.exists() and not project_dir.is_dir():
        _bail("--output for sam-app must be a directory")
    if project_dir.exists() and not overwrite:
        _bail(f"{project_dir} already exists; pass --overwrite to reuse it")
    project_dir.mkdir(parents=True, exist_ok=True)

    asset_dir = project_dir / SAM_ASSET_SUBDIR
    if asset_dir.exists():
        shutil.rmtree(asset_dir)

    template_path = project_dir / SAM_TEMPLATE_NAME
    source, override = _prepare_source(input_path, stack_name, check=False, diff=False)
    options = TemplateProcessingOptions(
        column=column,
        flow_style=flow_style,
        cdk_clean=True,
        cdk_rename=True,
        cdk_semantic_naming=True,
        cdk_keep_path_metadata=True,
        cdk_collision_strategy="numbered",
        cdk_samify=True,
        samify_relative_base=project_dir,
        samify_prefer_external=prefer_external,
    )
    stager = SamAssetStager(project_dir, assets_subdir=SAM_ASSET_SUBDIR)
    result = _run_pipeline(source, options, override, sam_asset_stager=stager)

    if result.lint_errors:
        for issue in result.lint_errors:
            click.echo(_format_lint_issue(issue), err=True)
        if not ignore_errors:
            click.echo(
                "Lint errors detected; use --ignore-errors to write output anyway.", err=True
            )
            sys.exit(1)
        click.echo("Lint errors detected but ignored.", err=True)

    normalized = (
        result.formatted_content
        if result.formatted_content.endswith("\n")
        else result.formatted_content + "\n"
    )
    template_path.write_text(normalized)
    click.echo(f"✓ SAM template written to {template_path}", err=True)
    click.echo(f"✓ Staged assets under {asset_dir}", err=True)
    if plan_path:
        _write_plan(
            plan_path,
            target="sam-app",
            strategy=strategy,
            result=result,
            outputs={
                "template": str(template_path),
                "assets": str(asset_dir),
            },
        )
    if lint and result.lint_warnings:
        for issue in result.lint_warnings:
            click.echo(_format_lint_issue(issue), err=True)


def _run_report_refactor(
    *,
    input_path: Optional[Path],
    output: Optional[Path],
    column: int,
    flow_style: str,
    stack_name: Optional[str],
    lint: bool,
    strategy: str,
    plan_path: Optional[Path],
) -> None:
    source, override = _prepare_source(input_path, stack_name, check=False, diff=False)
    options = TemplateProcessingOptions(column=column, flow_style=flow_style)
    result = _run_pipeline(source, options, override)
    lint_failed = _print_lint_findings(result, lint)
    report = {
        "source": result.source_name,
        "changed": result.changed,
        "cdk_cleaned": result.cdk_cleaned,
        "samified": result.samified,
        "messages": [{"level": m.level, "text": m.text} for m in result.messages],
        "summary": result.summary,
        "traits": result.traits,
    }
    serialized = json.dumps(report, indent=2, sort_keys=True)
    target = output
    if target is None:
        click.echo(serialized)
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(serialized + "\n")
        click.echo(f"✓ Report written to {target}", err=True)
    if plan_path:
        _write_plan(plan_path, target="report-only", strategy=strategy, result=result)
    if lint_failed:
        sys.exit(1)


def _write_plan(
    path: Path,
    *,
    target: str,
    strategy: str,
    result: TemplateProcessingResult,
    outputs: Optional[dict[str, str]] = None,
) -> None:
    payload = {
        "target": target,
        "strategy": strategy,
        "source": result.source_name,
        "cdk_cleaned": result.cdk_cleaned,
        "samified": result.samified,
        "messages": [{"level": m.level, "text": m.text} for m in result.messages],
        "assets": [
            {
                "logical_id": asset.logical_id,
                "source_path": str(asset.source_path),
                "staged_path": str(asset.staged_path),
            }
            for asset in result.sam_assets
        ],
    }
    if outputs:
        payload["outputs"] = outputs
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    click.echo(f"✓ Plan written to {path}", err=True)


if __name__ == "__main__":
    dispatch_cli()
