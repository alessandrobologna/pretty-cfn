"""FastMCP server exposing Pretty CFN capabilities."""

from __future__ import annotations

import difflib
import json
from pathlib import Path
from textwrap import dedent
from typing import Annotated, Any, Dict, List, Literal, Optional, TypedDict

from fastmcp import Context, FastMCP
from mcp.types import TextContent
from pydantic import Field

from .service import (
    LintIssue,
    ProcessingMessage,
    TemplateProcessingError,
    TemplateProcessingOptions,
    TemplateProcessingResult,
    TemplateSource,
    lint_template,
    process_template,
    process_file,
)

from .agents.refactor_workflow import RefactorRequest, run_refactor_stage


class LintIssueDict(TypedDict):
    rule_id: str
    message: str
    filename: str
    line: int
    column: int
    severity: str


class MessageDict(TypedDict):
    level: str
    text: str


class WritePlanDict(TypedDict):
    path: str
    content: str
    mode: Literal["overwrite", "create"]


class FormatResult(TypedDict):
    source_name: str
    formatted_content: str
    rename_map: Dict[str, str]
    lint_warnings: List[LintIssueDict]
    lint_errors: List[LintIssueDict]
    messages: List[MessageDict]
    structure_changed: bool
    samified: bool
    cdk_cleaned: bool
    diff: str
    changed: bool
    summary: Dict[str, Any]
    traits: Dict[str, Any]
    writes: List[WritePlanDict]


INSTRUCTIONS = dedent(
    """
    Pretty CFN formats CloudFormation/SAM templates and converts CDK stacks to SAM.

    TOOL SELECTION
    - format_*: Format templates without architectural changes
    - refactor_*: Convert CDK to SAM (use format_* on output for custom formatting)
    - lint_template: Run cfn-lint only
    - list_stacks / find_stacks: Discover deployed stacks

    IMPORTANT
    - Use refactor_* (not format_*) for CDK-to-SAM conversion
    - Prefer replace=false to preview changes before writing
    """
)


mcp = FastMCP("Pretty CFN", instructions=INSTRUCTIONS)


def _serialize_lint(issues: List[LintIssue]) -> List[LintIssueDict]:
    serialized: List[LintIssueDict] = []
    for issue in issues:
        serialized.append(
            LintIssueDict(
                rule_id=issue.rule_id,
                message=issue.message,
                filename=issue.filename,
                line=issue.line,
                column=issue.column,
                severity=issue.severity,
            )
        )
    return serialized


def _serialize_messages(messages: List[ProcessingMessage]) -> List[MessageDict]:
    return [MessageDict(level=msg.level, text=msg.text) for msg in messages]


def _serialize_refactor_artifacts(artifacts) -> Dict[str, Any]:
    result = artifacts.result
    payload: Dict[str, Any] = {
        "formatted_template": result.formatted_content,
        "rename_map": result.rename_map,
        "lint": {
            "warnings": _serialize_lint(result.lint_warnings),
            "errors": _serialize_lint(result.lint_errors),
        },
        "summary": result.summary,
        "diff": result.diff,
        "sam_assets": [_asset_dict(asset) for asset in getattr(artifacts, "sam_assets", [])],
        "written_path": str(artifacts.written_path) if artifacts.written_path else None,
    }
    if not artifacts.written_path:
        writes = []
        for path, content, mode in getattr(artifacts, "asset_writes", []):
            writes.append(
                {
                    "path": str(path),
                    "content": content if content.endswith("\n") else f"{content}\n",
                    "mode": mode,
                }
            )
        payload["write_plan"] = writes
    return payload


def _asset_dict(asset) -> dict[str, Any]:
    return {
        "logical_id": asset.logical_id,
        "source_path": str(asset.source_path),
        "staged_path": str(asset.staged_path),
    }


def _result_to_dict(
    result: TemplateProcessingResult,
    *,
    writes: Optional[List[WritePlanDict]] = None,
) -> FormatResult:
    return FormatResult(
        source_name=result.source_name,
        formatted_content=result.formatted_content,
        rename_map=result.rename_map,
        lint_warnings=_serialize_lint(result.lint_warnings),
        lint_errors=_serialize_lint(result.lint_errors),
        messages=_serialize_messages(result.messages),
        structure_changed=result.structure_changed,
        samified=result.samified,
        cdk_cleaned=result.cdk_cleaned,
        diff=result.diff,
        changed=result.changed,
        summary=result.summary,
        traits=result.traits,
        writes=writes or [],
    )


def _build_options(**kwargs: Any) -> TemplateProcessingOptions:
    """Build TemplateProcessingOptions from keyword arguments, ignoring None values."""
    opts = TemplateProcessingOptions()
    for key, value in kwargs.items():
        if value is not None:
            if key in {"cdk_out", "cdk_manifest", "cdk_tree", "samify_relative_base"}:
                setattr(opts, key, Path(value))
            else:
                setattr(opts, key, value)
    return opts


def _format_template_impl(
    content: str, options: TemplateProcessingOptions
) -> TemplateProcessingResult:
    source = TemplateSource(path=None, stack_name=None, inline_content=content)
    return process_template(source, options)


def _process_stack(stack_name: str, options: TemplateProcessingOptions) -> TemplateProcessingResult:
    source = TemplateSource(path=None, stack_name=stack_name, inline_content=None)
    return process_template(source, options)


@mcp.tool()
async def format_template(
    content: Annotated[
        str,
        Field(description="The CloudFormation or SAM template content (YAML or JSON) to format"),
    ],
    ctx: Context,
    column: Annotated[
        Optional[int], Field(description="Column number for value alignment (default: 40)")
    ] = None,
    flow_style: Annotated[
        Optional[str],
        Field(description="YAML flow style: 'block' (default) or 'compact' for inline {} and []"),
    ] = None,
) -> FormatResult:
    """Run the full formatter pipeline on inline content."""

    await ctx.report_progress(0.1, 1.0, "Formatting template...")
    opts = _build_options(column=column, flow_style=flow_style)
    try:
        result = _format_template_impl(content, opts)
    except TemplateProcessingError as exc:
        raise RuntimeError(str(exc)) from exc
    await ctx.report_progress(1.0, 1.0, "Formatting complete")
    return _result_to_dict(result)


@mcp.tool()
async def format_local_template(
    path: Annotated[
        str, Field(description="Path to the template file (must be accessible to the MCP server)")
    ],
    ctx: Context,
    replace: Annotated[
        bool, Field(description="If true, overwrites the original file with formatted content")
    ] = False,
    output_path: Annotated[
        Optional[str],
        Field(
            description="Optional path to write the formatted template to (if not using replace)"
        ),
    ] = None,
    column: Annotated[
        Optional[int], Field(description="Column number for value alignment (default: 40)")
    ] = None,
    flow_style: Annotated[
        Optional[str],
        Field(description="YAML flow style: 'block' (default) or 'compact' for inline {} and []"),
    ] = None,
) -> FormatResult:
    """Format a template from disk. Set replace=True to overwrite the file."""

    file_path = Path(path)
    if not file_path.exists():
        raise RuntimeError(f"File not found: {path}")

    await ctx.report_progress(0.05, 1.0, f"Reading {file_path.name}")
    opts = _build_options(column=column, flow_style=flow_style)
    try:
        result = process_file(file_path, opts)
    except TemplateProcessingError as exc:
        raise RuntimeError(str(exc)) from exc

    writes: List[WritePlanDict] = []
    target_path = None

    if output_path:
        target_path = Path(output_path)
    elif replace:
        target_path = file_path

    if target_path:
        if not target_path.parent.exists():
            target_path.parent.mkdir(parents=True, exist_ok=True)
        _write_text(target_path, result.formatted_content)
        writes.append(_build_write_plan(target_path, result.formatted_content))

    await ctx.report_progress(1.0, 1.0, "Formatting complete")
    return _result_to_dict(result, writes=writes)


@mcp.tool(name="lint_template")
def lint_template_tool(
    content: Annotated[
        str,
        Field(
            description="The CloudFormation or SAM template content (YAML or JSON) to lint with cfn-lint"
        ),
    ],
) -> Dict[str, List[LintIssueDict]]:
    """Return cfn-lint warnings/errors without formatting."""

    warnings, errors = lint_template(content, "<inline>")
    return {
        "warnings": _serialize_lint(warnings),
        "errors": _serialize_lint(errors),
    }


@mcp.tool()
async def format_deployed_stack(
    stack_name: Annotated[
        str, Field(description="Name of the deployed CloudFormation stack to fetch and format")
    ],
    ctx: Context,
    output_path: Annotated[
        Optional[str],
        Field(
            description="Optional path for the formatted template; returns a write plan if provided"
        ),
    ] = None,
    column: Annotated[
        Optional[int], Field(description="Column number for value alignment (default: 40)")
    ] = None,
    flow_style: Annotated[
        Optional[str],
        Field(description="YAML flow style: 'block' (default) or 'compact' for inline {} and []"),
    ] = None,
) -> FormatResult:
    """Fetch and format a deployed CloudFormation stack (maintains original architecture, does NOT convert to SAM)."""

    await ctx.report_progress(0.1, 1.0, f"Formatting stack {stack_name}")
    opts = _build_options(column=column, flow_style=flow_style)
    try:
        result = _process_stack(stack_name, opts)
    except TemplateProcessingError as exc:
        raise RuntimeError(str(exc)) from exc
    await ctx.report_progress(1.0, 1.0, "Stack formatting complete")
    writes: List[WritePlanDict] = []
    if output_path:
        path = Path(output_path)
        if not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
        _write_text(path, result.formatted_content)
        writes.append(_build_write_plan(path, result.formatted_content))
    return _result_to_dict(result, writes=writes)


@mcp.tool()
def refactor_local_app(
    path: Annotated[str, Field(description="Path to the CDK app or template file on disk")],
    output_path: Annotated[
        str,
        Field(description="Path where the SAM application should be written (must be a directory)"),
    ],
) -> Dict[str, Any]:
    """Refactor a local CDK application or template file into a SAM application.

    This tool converts CDK/CloudFormation templates to SAM format. For custom
    formatting options, call format_local_template on the output afterwards.
    """

    normalized_output_path = _normalize_output_path_param(output_path)

    opts = _build_options(
        cdk_clean=True,
        cdk_samify=True,
        cdk_rename=True,
    )

    request = RefactorRequest(
        path=Path(path),
        output_path=Path(normalized_output_path),
        options=opts,
    )
    artifacts = run_refactor_stage(request)
    return {"artifacts": _serialize_refactor_artifacts(artifacts)}


@mcp.tool()
def refactor_deployed_stack(
    stack_name: Annotated[
        str,
        Field(
            description="Name of the deployed CloudFormation/CDK stack to refactor into a SAM application"
        ),
    ],
    output_path: Annotated[
        str,
        Field(description="Path where the SAM application should be written (must be a directory)"),
    ],
) -> Dict[str, Any]:
    """Refactor a deployed CDK stack or CloudFormation stack into a local SAM application.

    This tool converts CDK/CloudFormation stacks to SAM format. For custom
    formatting options, call format_local_template on the output afterwards.
    """

    normalized_output_path = _normalize_output_path_param(output_path)

    opts = _build_options(
        cdk_clean=True,
        cdk_samify=True,
        cdk_rename=True,
    )

    request = RefactorRequest(
        stack_name=stack_name,
        output_path=Path(normalized_output_path),
        options=opts,
    )
    artifacts = run_refactor_stage(request)
    return {"artifacts": _serialize_refactor_artifacts(artifacts)}


def _normalize_output_path_param(path_str: str) -> str:
    """Ensure output path is a directory, stripping filename if necessary."""
    path = Path(path_str)
    if path.suffix.lower() in {".yaml", ".yml", ".json"}:
        return str(path.parent)
    return path_str


def _write_text(path: Path, content: str) -> None:
    text = content if content.endswith("\n") else content + "\n"
    path.write_text(text)


def _build_write_plan(
    path: Path, content: str, mode: Literal["overwrite", "create"] = "overwrite"
) -> WritePlanDict:
    normalized = path.resolve()
    payload = content if content.endswith("\n") else f"{content}\n"
    return WritePlanDict(path=str(normalized), content=payload, mode=mode)


def _iso_or_none(dt: Any) -> Optional[str]:
    """Convert datetime to ISO string, or return None if dt is None."""
    return dt.isoformat() if dt else None


@mcp.tool()
def list_stacks(
    name_prefix: Annotated[
        Optional[str],
        Field(description="Optional name prefix to filter stacks (matches start of StackName)"),
    ] = None,
    status_filter: Annotated[
        Optional[List[str]],
        Field(
            description="Optional list of stack status filters (e.g. ['CREATE_COMPLETE','UPDATE_COMPLETE'])"
        ),
    ] = None,
    max_results: Annotated[
        Optional[int],
        Field(description="Maximum number of stacks to return (server may cap this further)"),
    ] = None,
) -> Dict[str, Any]:
    """List CloudFormation stacks for selection in refactor/format tools.

    This tool is intentionally simple and deterministic. It defers fuzzy
    matching and stack choice to the caller/model. Use name_prefix to
    narrow the list when the user has a partial name.
    """

    try:
        import boto3
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "boto3 is required to list CloudFormation stacks from this MCP server"
        ) from exc

    client = boto3.client("cloudformation")

    effective_status_filter = status_filter or [
        "CREATE_COMPLETE",
        "UPDATE_COMPLETE",
        "IMPORT_COMPLETE",
        "ROLLBACK_COMPLETE",
    ]

    effective_max_results = max_results or 100
    collected: List[Dict[str, Any]] = []
    next_token: Optional[str] = None

    while True:
        kwargs: Dict[str, Any] = {"StackStatusFilter": effective_status_filter}
        if next_token:
            kwargs["NextToken"] = next_token

        response = client.list_stacks(**kwargs)
        for summary in response.get("StackSummaries", []):
            name = summary.get("StackName", "")
            if name_prefix and not name.startswith(name_prefix):
                continue
            collected.append(
                {
                    "stack_name": name,
                    "stack_id": summary.get("StackId", ""),
                    "status": summary.get("StackStatus", ""),
                    "creation_time": _iso_or_none(summary.get("CreationTime")),
                    "last_updated_time": _iso_or_none(summary.get("LastUpdatedTime")),
                    "deletion_time": _iso_or_none(summary.get("DeletionTime")),
                }
            )
            if len(collected) >= effective_max_results:
                return {
                    "stacks": collected,
                    "truncated": True,
                    "next_token": response.get("NextToken"),
                }

        next_token = response.get("NextToken")
        if not next_token:
            break

    return {
        "stacks": collected,
        "truncated": False,
        "next_token": None,
    }


@mcp.tool()
async def find_stacks(
    query: Annotated[
        str, Field(description="Partial stack name or phrase to match against StackName")
    ],
    ctx: Context,
    status_filter: Annotated[
        Optional[List[str]],
        Field(
            description="Optional list of stack status filters (e.g. ['CREATE_COMPLETE','UPDATE_COMPLETE'])"
        ),
    ] = None,
    max_results: Annotated[
        Optional[int], Field(description="Maximum number of matches to return (default: 10)")
    ] = None,
) -> Dict[str, Any]:
    """Find CloudFormation stacks whose names best match a free-text query.

    This tool performs deterministic fuzzy matching on stack names using a
    similarity score. It is intended to complement list_stacks by helping
    callers narrow down to likely candidates when the user only remembers
    part of the name.
    """

    try:
        import boto3
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "boto3 is required to list CloudFormation stacks from this MCP server"
        ) from exc

    client = boto3.client("cloudformation")

    effective_status_filter = status_filter or [
        "CREATE_COMPLETE",
        "UPDATE_COMPLETE",
        "IMPORT_COMPLETE",
        "ROLLBACK_COMPLETE",
    ]

    effective_max_results = max_results or 10

    summaries: List[Dict[str, Any]] = []
    next_token: Optional[str] = None

    while True:
        kwargs: Dict[str, Any] = {"StackStatusFilter": effective_status_filter}
        if next_token:
            kwargs["NextToken"] = next_token

        response = client.list_stacks(**kwargs)
        summaries.extend(response.get("StackSummaries", []))
        next_token = response.get("NextToken")
        if not next_token:
            break

    if not summaries:
        return {"query": query, "stacks": []}

    # Deterministic baseline scoring with difflib
    query_lower = query.lower()
    scored: List[tuple[float, Dict[str, Any]]] = []
    for summary in summaries:
        name = summary.get("StackName") or ""
        if not name:
            continue
        score = difflib.SequenceMatcher(None, query_lower, name.lower()).ratio()
        scored.append((score, summary))

    scored.sort(key=lambda item: item[0], reverse=True)

    # Try to refine using sampling if available; fall back silently on failure.
    selected_summaries: List[Dict[str, Any]] = []
    try:
        # Limit how many candidates we send through the prompt for efficiency.
        candidate_limit = min(len(scored), 100)
        candidate_block_lines: List[str] = []
        name_to_summary: Dict[str, Dict[str, Any]] = {}
        for idx, (score, summary) in enumerate(scored[:candidate_limit], start=1):
            name = summary.get("StackName", "")
            status = summary.get("StackStatus", "")
            candidate_block_lines.append(f"{idx}. {name} [{status}]")
            if name:
                name_to_summary[name] = summary

        prompt = (
            "You are helping select CloudFormation stacks based on a user query.\n\n"
            f"User query: {query!r}\n\n"
            "Here is a numbered list of candidate stacks in the format:\n"
            "index. StackName [StackStatus]\n\n"
            f"{chr(10).join(candidate_block_lines)}\n\n"
            f"Return a JSON array of up to {effective_max_results} stack names "
            "(exact StackName strings) ordered from best to worst match. "
            "Do not include any explanation, only the JSON array."
        )

        content = await ctx.sample([prompt])
        if isinstance(content, TextContent):
            try:
                names = json.loads(content.text)
                if isinstance(names, list):
                    for name in names:
                        if not isinstance(name, str):
                            continue
                        summary = name_to_summary.get(name)
                        if summary is not None:
                            selected_summaries.append(summary)
                            if len(selected_summaries) >= effective_max_results:
                                break
            except Exception:
                # If parsing fails, we will fall back to deterministic ranking.
                selected_summaries = []
    except Exception:
        selected_summaries = []

    # If sampling did not yield usable results, take top-N from deterministic scoring.
    if not selected_summaries:
        selected_summaries = [summary for _, summary in scored[:effective_max_results]]

    stacks: List[Dict[str, Any]] = []
    for summary in selected_summaries:
        name = summary.get("StackName", "")
        score = difflib.SequenceMatcher(None, query_lower, name.lower()).ratio()
        stacks.append(
            {
                "stack_name": name,
                "stack_id": summary.get("StackId", ""),
                "status": summary.get("StackStatus", ""),
                "creation_time": _iso_or_none(summary.get("CreationTime")),
                "last_updated_time": _iso_or_none(summary.get("LastUpdatedTime")),
                "deletion_time": _iso_or_none(summary.get("DeletionTime")),
                "score": score,
            }
        )

    return {
        "query": query,
        "stacks": stacks,
    }


def main() -> None:
    """Run the MCP server over stdio."""

    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
