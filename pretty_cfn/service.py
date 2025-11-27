"""Shared template-processing pipeline for CLI and MCP surfaces."""

from __future__ import annotations

from collections import OrderedDict
import json
from dataclasses import dataclass, field
import difflib
import io
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import urllib.request

from ruamel.yaml import YAML

from .cdk_cleaner import CDKCleaner
from .exceptions import TemplateProcessingError
from .formatter import (
    create_cfn_yaml,
    format_cfn_yaml,
    convert_stepfunction_definitions,
    convert_joins_to_sub,
    ensure_cfn_tags,
    normalize_template_text,
    _to_ordered_dict,
    _mark_literal_blocks,
    _collect_resource_titles,
)
from .samifier import (
    SamAssetRecord,
    SamAssetStager,
    samify_template,
    rewrite_function_url_refs,
    _rewrite_code_uri_paths,
    _prepare_inline_code,
)
from .samifier.optimizations import (
    apply_function_globals,
    convert_simple_tables,
    strip_cdk_metadata,
)


@dataclass
class TemplateSource:
    """Describe where a template should be loaded from."""

    path: Optional[Path] = None
    stack_name: Optional[str] = None
    inline_content: Optional[str] = None

    def load(self) -> Tuple[str, str]:
        """Return (content, logical_name)."""

        if self.stack_name:
            content = _fetch_stack_template(self.stack_name)
            return content, f"stack:{self.stack_name}"

        if self.inline_content is not None:
            name = str(self.path) if self.path else "<stdin>"
            return self.inline_content, name

        if self.path:
            if self.path.is_dir():
                # Try to resolve a template if a directory was provided
                candidate = self.path / "cdk.out"
                search_dir = candidate if candidate.is_dir() else self.path

                # Find all template files
                templates = list(search_dir.glob("*.template.json"))

                if len(templates) == 1:
                    self.path = templates[0]
                elif len(templates) > 1:
                    names = ", ".join([t.name for t in templates])
                    raise TemplateProcessingError(
                        f"Multiple templates found in {search_dir}: {names}. Please specify the template file directly."
                    )
                else:
                    raise TemplateProcessingError(
                        f"No templates (*.template.json) found in {search_dir}. Did you run 'cdk synth'?"
                    )

            try:
                content = self.path.read_text()
            except FileNotFoundError as exc:  # pragma: no cover - pass through
                raise TemplateProcessingError(f"File {self.path} does not exist") from exc
            return content, str(self.path)

        raise TemplateProcessingError("No template source specified")


@dataclass
class TemplateProcessingOptions:
    """Tunables for the processing pipeline."""

    column: int = 40
    flow_style: str = "block"
    cdk_clean: bool = False
    cdk_keep_hashes: bool = False
    cdk_rename: Optional[bool] = None
    cdk_semantic_naming: bool = True
    cdk_keep_path_metadata: bool = True
    cdk_collision_strategy: str = "numbered"
    cdk_out: Optional[Path] = None
    cdk_manifest: Optional[Path] = None
    cdk_tree: Optional[Path] = None
    cdk_samify: bool = False
    samify_relative_base: Optional[Path] = None
    samify_prefer_external: bool = False


@dataclass
class LintIssue:
    """Structured representation of a cfn-lint finding."""

    rule_id: str
    message: str
    filename: str
    line: int
    column: int
    severity: str  # "error" or "warning"


@dataclass
class ProcessingMessage:
    """Generic informational or warning message emitted during processing."""

    level: str
    text: str


@dataclass
class TemplateProcessingResult:
    """Return object for the processing pipeline."""

    source_name: str
    original_content: str
    formatted_content: str
    rename_map: Dict[str, str] = field(default_factory=dict)
    lint_warnings: List[LintIssue] = field(default_factory=list)
    lint_errors: List[LintIssue] = field(default_factory=list)
    messages: List[ProcessingMessage] = field(default_factory=list)
    structure_changed: bool = False
    samified: bool = False
    cdk_cleaned: bool = False
    detected_cdk_out: Optional[Path] = None
    asset_search_paths: List[Path] = field(default_factory=list)
    diff: str = ""
    changed: bool = False
    summary: Dict[str, Any] = field(default_factory=dict)
    traits: Dict[str, Any] = field(default_factory=dict)
    sam_assets: List[SamAssetRecord] = field(default_factory=list)


def process_template(
    source: TemplateSource,
    options: TemplateProcessingOptions,
    *,
    sam_asset_stager: Optional[SamAssetStager] = None,
) -> TemplateProcessingResult:
    """Run the full Pretty CFN pipeline and return artifacts."""

    data, original_content, normalized_content, source_name = _load_and_parse_template(source)

    data, structure_changed, traits, resource_title_map = _apply_structural_normalizations(data)

    asset_search_paths, inferred_cdk_out = _prepare_asset_context(source, options)

    messages: List[ProcessingMessage] = []

    relative_base = options.samify_relative_base or source.path or Path.cwd()

    data, structure_changed, normalized_content, samified = _samify_before_cdk(
        data=data,
        options=options,
        asset_search_paths=asset_search_paths,
        relative_base=relative_base,
        sam_asset_stager=sam_asset_stager,
        resource_title_map=resource_title_map,
        structure_changed=structure_changed,
        normalized_content=normalized_content,
    )

    cleaned_content, rename_map, cdk_cleaned, samified = _apply_cdk_and_samify(
        data=data,
        options=options,
        asset_search_paths=asset_search_paths,
        relative_base=relative_base,
        sam_asset_stager=sam_asset_stager,
        resource_title_map=resource_title_map,
        messages=messages,
        inferred_cdk_out=inferred_cdk_out,
        normalized_content=normalized_content,
        samified=samified,
    )

    formatted_content = format_cfn_yaml(
        cleaned_content,
        alignment_column=options.column,
        resource_titles=resource_title_map,
        flow_style=options.flow_style,
    )

    lint_warnings, lint_errors = lint_template(formatted_content, source_name)
    if options.cdk_samify:
        lint_errors = _suppress_websocket_event_errors(lint_errors)

    diff_lines = difflib.unified_diff(
        original_content.splitlines(keepends=True),
        formatted_content.splitlines(keepends=True),
        fromfile=source_name,
        tofile=f"{source_name} (formatted)",
    )
    diff_text = "".join(diff_lines)
    changed = formatted_content != original_content
    summary = {
        "original_lines": len(original_content.splitlines()),
        "formatted_lines": len(formatted_content.splitlines()),
        "line_delta": len(formatted_content.splitlines()) - len(original_content.splitlines()),
        "lint_warning_count": len(lint_warnings),
        "lint_error_count": len(lint_errors),
    }
    summary.update(traits)

    return TemplateProcessingResult(
        source_name=source_name,
        original_content=original_content,
        formatted_content=formatted_content,
        rename_map=rename_map,
        lint_warnings=lint_warnings,
        lint_errors=lint_errors,
        messages=messages,
        structure_changed=structure_changed,
        samified=samified,
        cdk_cleaned=cdk_cleaned,
        detected_cdk_out=inferred_cdk_out,
        asset_search_paths=asset_search_paths,
        diff=diff_text,
        changed=changed,
        summary=summary,
        traits=traits,
        sam_assets=list(sam_asset_stager.records) if sam_asset_stager else [],
    )


def _load_and_parse_template(
    source: TemplateSource,
) -> Tuple[Any, str, str, str]:
    original_content, source_name = source.load()
    normalized_content = normalize_template_text(original_content)

    try:
        yaml_instance = create_cfn_yaml()
        data = yaml_instance.load(normalized_content)
    except Exception as exc:  # pragma: no cover - yaml library
        raise TemplateProcessingError(f"Invalid YAML in {source_name}: {exc}") from exc

    return data, original_content, normalized_content, source_name


def _apply_structural_normalizations(
    data: Any,
) -> Tuple[Any, bool, Dict[str, Any], Dict[str, str]]:
    data = convert_stepfunction_definitions(data)
    structure_changed = _strip_empty_sections(data)
    if _normalize_appsync_definitions(data):
        structure_changed = True
    traits = _detect_template_traits(data)
    resource_title_map: Dict[str, str] = {}
    _merge_resource_titles(resource_title_map, data)
    return data, structure_changed, traits, resource_title_map


def _prepare_asset_context(
    source: TemplateSource,
    options: TemplateProcessingOptions,
) -> Tuple[List[Path], Optional[Path]]:
    inferred_cdk_out: Optional[Path] = None
    if (options.cdk_clean or options.cdk_samify) and not options.cdk_out:
        inferred_cdk_out = _discover_cdk_out(source.path)

    asset_search_paths: List[Path] = []
    for candidate in (options.cdk_out, inferred_cdk_out):
        if candidate and candidate not in asset_search_paths:
            asset_search_paths.append(candidate)
    if source.path:
        parent = source.path.parent
        if parent not in asset_search_paths:
            asset_search_paths.append(parent)
    cwd = Path.cwd()
    if cwd not in asset_search_paths:
        asset_search_paths.append(cwd)

    return asset_search_paths, inferred_cdk_out


def _samify_before_cdk(
    *,
    data: Any,
    options: TemplateProcessingOptions,
    asset_search_paths: List[Path],
    relative_base: Path,
    sam_asset_stager: Optional[SamAssetStager],
    resource_title_map: Dict[str, str],
    structure_changed: bool,
    normalized_content: str,
) -> Tuple[Any, bool, str, bool]:
    samified = False

    if options.cdk_samify and not options.cdk_clean:
        data, samified = samify_template(
            data,
            asset_search_paths=[p for p in asset_search_paths if p],
            relative_to=relative_base,
            asset_stager=sam_asset_stager,
            prefer_external_assets=options.samify_prefer_external,
        )
        if not options.cdk_clean:
            convert_simple_tables(data)
            apply_function_globals(data)
            _merge_resource_titles(resource_title_map, data)
            strip_cdk_metadata(data)
        structure_changed = structure_changed or _strip_empty_sections(data) or samified
        if structure_changed and not options.cdk_clean:
            normalized_content = _dump_cfn_data(data, options.column)
    else:
        if structure_changed and not options.cdk_clean:
            normalized_content = _dump_cfn_data(data, options.column)

    return data, structure_changed, normalized_content, samified


def _apply_cdk_and_samify(
    *,
    data: Any,
    options: TemplateProcessingOptions,
    asset_search_paths: List[Path],
    relative_base: Path,
    sam_asset_stager: Optional[SamAssetStager],
    resource_title_map: Dict[str, str],
    messages: List[ProcessingMessage],
    inferred_cdk_out: Optional[Path],
    normalized_content: str,
    samified: bool,
) -> Tuple[str, Dict[str, str], bool, bool]:
    cleaned_content = normalized_content
    rename_map: Dict[str, str] = {}
    cdk_cleaned = False

    if options.cdk_clean:
        cdk_metadata: Optional[Path] = (
            options.cdk_out or options.cdk_manifest or options.cdk_tree or inferred_cdk_out
        )

        if inferred_cdk_out and inferred_cdk_out == cdk_metadata and options.cdk_out is None:
            messages.append(
                ProcessingMessage(
                    level="info",
                    text=f"Auto-detected cdk.out directory at {inferred_cdk_out}",
                )
            )

        is_cdk_template = _looks_like_cdk_template(data)

        cleaner = CDKCleaner(
            mode="readable" if (options.cdk_rename is not False) else "deployable",
            rename_logical_ids=True if options.cdk_rename is None else options.cdk_rename,
            strip_hashes=not options.cdk_keep_hashes,
            semantic_naming=options.cdk_semantic_naming,
            keep_path_metadata=options.cdk_keep_path_metadata,
            # When samifying CDK templates, keep aws:asset metadata so the SAM
            # converters can stage local assets into the SAM project. For
            # non-CDK templates we retain the previous behavior of stripping
            # asset metadata.
            strip_asset_metadata=False if (options.cdk_samify and is_cdk_template) else None,
            collision_strategy=options.cdk_collision_strategy,
            cdk_metadata=cdk_metadata,
        )

        try:
            cleaned_data = cleaner.clean(data)
        except Exception as exc:  # pragma: no cover - CDK cleaner internals
            raise TemplateProcessingError(f"Error during CDK cleaning: {exc}") from exc

        if not is_cdk_template:
            messages.append(
                ProcessingMessage(
                    level="info",
                    text="cdk-clean requested but no CDK markers were detected.",
                )
            )

        _strip_empty_sections(cleaned_data)
        _strip_bootstrap_rule(cleaned_data)
        _strip_empty_sections(cleaned_data)

        rename_map = cleaner.get_rename_map() or {}
        if rename_map:
            rewrite_function_url_refs(cleaned_data, rename_map)
            if sam_asset_stager and sam_asset_stager.records:
                sam_asset_stager.apply_rename_map(rename_map)
                _rewrite_code_uri_paths(cleaned_data, sam_asset_stager, relative_base)

        if options.cdk_samify:
            cleaned_data, sam_changed = samify_template(
                cleaned_data,
                asset_search_paths=[p for p in asset_search_paths if p],
                relative_to=relative_base,
                asset_stager=sam_asset_stager,
                prefer_external_assets=options.samify_prefer_external,
            )
            samified = samified or sam_changed
            convert_simple_tables(cleaned_data)
            apply_function_globals(cleaned_data)
            _merge_resource_titles(resource_title_map, cleaned_data)
            strip_cdk_metadata(cleaned_data)

        ordered = ensure_cfn_tags(_to_ordered_dict(cleaned_data))
        ordered = convert_stepfunction_definitions(ordered)
        ordered = convert_joins_to_sub(ordered)
        _mark_literal_blocks(ordered)

        dumper_stream = io.StringIO()
        dump_yaml = create_cfn_yaml()
        dump_yaml.dump(ordered, dumper_stream)
        cleaned_content = dumper_stream.getvalue()
        cdk_cleaned = True

    return cleaned_content, rename_map, cdk_cleaned, samified


def _normalize_appsync_definitions(template: dict) -> bool:
    resources = template.get("Resources")
    if not isinstance(resources, dict):
        return False
    changed = False
    for resource in resources.values():
        if not isinstance(resource, dict):
            continue
        if resource.get("Type") != "AWS::AppSync::GraphQLSchema":
            continue
        properties = resource.get("Properties")
        if not isinstance(properties, dict):
            continue
        definition = properties.get("Definition")
        if not isinstance(definition, str):
            continue
        normalized = _prepare_inline_code(definition)
        if normalized != definition:
            properties["Definition"] = normalized
            changed = True
    return changed


def process_file(
    path: Path,
    options: TemplateProcessingOptions,
) -> TemplateProcessingResult:
    source = TemplateSource(path=path)
    result = process_template(source, options)
    result.source_name = str(path)
    return result


def lint_template(content: str, template_name: str) -> Tuple[List[LintIssue], List[LintIssue]]:
    """Run cfn-lint and categorize warnings vs errors."""

    from cfnlint.api import ManualArgs, lint  # Local import to keep startup fast

    config = ManualArgs()
    matches = lint(content, config=config)
    warnings: List[LintIssue] = []
    errors: List[LintIssue] = []

    for match in matches:
        rule_id = (match.rule.id or "").upper()
        filename = getattr(match, "filename", None) or template_name
        severity_code = (rule_id[:1] if rule_id else "E").upper()
        issue = LintIssue(
            rule_id=rule_id or "?",
            message=getattr(match, "message", ""),
            filename=filename,
            line=getattr(match, "linenumber", -1),
            column=getattr(match, "columnnumber", -1),
            severity="error" if severity_code == "E" else "warning",
        )
        if issue.severity == "error":
            errors.append(issue)
        else:
            warnings.append(issue)

    return warnings, errors


def _merge_resource_titles(target: Dict[str, str], template_data: Any) -> None:
    if target is None:
        return
    titles = _collect_resource_titles(template_data)
    if not titles:
        return
    target.update(titles)


def import_stack_to_path(
    stack_name: str,
    target_path: Path,
    options: TemplateProcessingOptions,
    *,
    replace: bool = True,
) -> TemplateProcessingResult:
    content = fetch_stack_template(stack_name)
    source = TemplateSource(path=None, stack_name=None, inline_content=content)
    result = process_template(source, options)
    if replace or not target_path.exists():
        _write_text(target_path, result.formatted_content)
    result.source_name = str(target_path)
    return result


def _build_url_rename_map(cleaned_data: dict, rename_map: Dict[str, str]) -> Dict[str, str]:
    url_map: Dict[str, str] = {}
    resources_post = cleaned_data.get("Resources", {})
    if not isinstance(resources_post, dict):
        return url_map
    for old_id, new_id in rename_map.items():
        resource = resources_post.get(new_id)
        if not isinstance(resource, dict):
            continue
        if resource.get("Type") != "AWS::Serverless::Function":
            continue
        properties = resource.get("Properties")
        if not isinstance(properties, dict):
            continue
        if "FunctionUrlConfig" not in properties:
            continue
        url_map[f"{old_id}Url"] = f"{new_id}Url"
    return url_map


def _suppress_websocket_event_errors(issues: list[LintIssue]) -> list[LintIssue]:
    filtered: list[LintIssue] = []
    for issue in issues:
        message = issue.message or ""
        if (
            issue.rule_id == "E0001"
            and "Event with id [Ws" in message
            and "Property 'Path' is required" in message
        ):
            continue
        filtered.append(issue)
    return filtered


def fetch_stack_template(stack_name: str) -> str:
    """Public wrapper for fetching CloudFormation templates from a stack."""

    return _fetch_stack_template(stack_name)


def _fetch_stack_template(stack_name: str) -> str:
    try:
        import boto3
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise TemplateProcessingError(
            "boto3 is required to fetch templates from CloudFormation"
        ) from exc

    session = boto3.session.Session()
    client = session.client("cloudformation")

    try:
        response = client.get_template(StackName=stack_name, TemplateStage="Original")
    except Exception as exc:  # pragma: no cover - AWS errors
        raise TemplateProcessingError(
            f"Unable to fetch template for stack {stack_name}: {exc}"
        ) from exc

    body = response.get("TemplateBody")
    if isinstance(body, dict):
        normalized = json.loads(json.dumps(body))
        safe_yaml = YAML(typ="safe")
        safe_yaml.default_flow_style = False
        stream = io.StringIO()
        safe_yaml.dump(normalized, stream)
        return stream.getvalue()
    if isinstance(body, str) and body.strip():
        return body

    url = response.get("TemplateURL") or response.get("TemplateBodyS3Url")
    if url:
        try:
            with urllib.request.urlopen(url) as handle:
                return handle.read().decode("utf-8")
        except Exception as exc:  # pragma: no cover - network IO
            raise TemplateProcessingError(f"Failed to download template from {url}: {exc}") from exc

    raise TemplateProcessingError(
        f"CloudFormation did not return a template body for stack {stack_name}"
    )


def _discover_cdk_out(input_path: Optional[Path]) -> Optional[Path]:
    search_roots: List[Path] = []
    if input_path is not None:
        search_roots.append(input_path.parent)
    search_roots.append(Path.cwd())

    for root in search_roots:
        if root.name == "cdk.out" and root.is_dir():
            return root
        current = root
        for candidate_parent in [current] + list(current.parents):
            candidate = candidate_parent / "cdk.out"
            if candidate.is_dir():
                return candidate
    return None


EMPTY_TOP_LEVEL_SECTIONS = ("Conditions", "Parameters", "Outputs", "Rules")


def _strip_empty_sections(data, sections=EMPTY_TOP_LEVEL_SECTIONS) -> bool:
    changed = False
    if not isinstance(data, (dict, OrderedDict)):
        return False
    for key in sections:
        value = data.get(key)
        if isinstance(value, dict) and not value:
            del data[key]
            changed = True
    return changed


def _strip_bootstrap_rule(data) -> bool:
    if not isinstance(data, dict):
        return False
    rules = data.get("Rules")
    if not isinstance(rules, dict):
        return False
    if "CheckBootstrapVersion" not in rules:
        return False
    del rules["CheckBootstrapVersion"]
    if not rules:
        del data["Rules"]
    return True


def _dump_cfn_data(data, column: int) -> str:
    ordered = ensure_cfn_tags(_to_ordered_dict(data))
    stream = io.StringIO()
    dump_yaml = create_cfn_yaml()
    dump_yaml.dump(ordered, stream)
    return stream.getvalue()


def _looks_like_cdk_template(data) -> bool:
    try:
        if isinstance(data, dict):
            resources = data.get("Resources")
            if isinstance(resources, dict):
                for body in resources.values():
                    if not isinstance(body, dict):
                        continue
                    if body.get("Type") == "AWS::CDK::Metadata":
                        return True
                    metadata = body.get("Metadata")
                    if isinstance(metadata, dict) and "aws:cdk:path" in metadata:
                        return True
        return False
    except Exception:  # pragma: no cover - defensive
        return False


def _write_text(path: Path, content: str) -> None:
    text = content if content.endswith("\n") else content + "\n"
    path.write_text(text)


def discover_cdk_metadata(base_path: Path) -> Dict[str, Optional[str]]:
    base = base_path if base_path.is_dir() else base_path.parent

    def find_upwards(name: str) -> Optional[Path]:
        current = base
        for parent in [current] + list(current.parents):
            candidate = parent / name
            if candidate.exists():
                return candidate
        return None

    cdk_out = find_upwards("cdk.out")
    manifest = None
    tree = None
    if cdk_out:
        manifest = cdk_out / "manifest.json"
        manifest = manifest if manifest.exists() else None
        tree = cdk_out / "tree.json"
        tree = tree if tree.exists() else None

    if manifest is None:
        manifest = find_upwards("manifest.json")
    if tree is None:
        tree = find_upwards("tree.json")

    return {
        "cdk_out": str(cdk_out) if cdk_out else None,
        "manifest": str(manifest) if manifest else None,
        "tree": str(tree) if tree else None,
    }


def _detect_template_traits(data: Any) -> Dict[str, Any]:
    traits: Dict[str, Any] = {
        "has_cdk_metadata": False,
        "has_sam_transform": False,
        "resource_count": 0,
    }

    if isinstance(data, dict):
        resources = data.get("Resources")
        if isinstance(resources, dict):
            traits["resource_count"] = len(resources)
            for resource in resources.values():
                if not isinstance(resource, dict):
                    continue
                if resource.get("Type") == "AWS::CDK::Metadata":
                    traits["has_cdk_metadata"] = True
                    break
                metadata = resource.get("Metadata")
                if isinstance(metadata, dict) and "aws:cdk:path" in metadata:
                    traits["has_cdk_metadata"] = True
                    break

        transform = data.get("Transform")
        if transform == "AWS::Serverless-2016-10-31" or (
            isinstance(transform, list) and "AWS::Serverless-2016-10-31" in transform
        ):
            traits["has_sam_transform"] = True

    return traits


def format_file_set(
    file_paths: List[Path],
    options: TemplateProcessingOptions,
    *,
    replace: bool = False,
) -> List[TemplateProcessingResult]:
    results: List[TemplateProcessingResult] = []
    for path in file_paths:
        res = process_file(path, options)
        results.append(res)
        if replace and res.changed:
            _write_text(path, res.formatted_content)
    return results
