"""Core formatting logic for CloudFormation YAML templates.

This module remains the public façade for formatter helpers while delegating
most intrinsic and layout mechanics to the internal formatter_intrinsics and
formatter_layout modules.
"""

from __future__ import annotations

import io
from collections import OrderedDict
from typing import Any, Dict, List, Optional

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq

from .formatter_intrinsics import (  # noqa: F401
    CFN_TAGS,
    CFNTag,
    CFNLoader,
    JSON_INTRINSIC_MAP,
    LiteralStr,
    create_cfn_yaml,
    normalize_template_text,
    _stringify_getatt_value,
    _to_ordered_dict,
    ensure_cfn_tags,
    convert_stepfunction_definitions,
    convert_joins_to_sub,
    _mark_literal_blocks,
)
from .formatter_layout import (
    _align_block_literals,
    _align_values,
    _fix_nested_cfntag_indent,
    _format_literal_blocks,
    _mark_compact_flow_style,
)


def format_cfn_yaml(
    content: str,
    alignment_column: int = 40,
    *,
    pre_normalized: bool = False,
    resource_titles: Optional[Dict[str, str]] = None,
    flow_style: str = "block",
) -> str:
    """
    Format a CloudFormation YAML template with aligned values.

    Args:
        content: The YAML content as a string
        alignment_column: The column position to align values to

    Returns:
        The formatted YAML as a string
    """
    if pre_normalized:
        normalized_content = content
    else:
        # Normalize line endings for consistent parsing
        normalized_content = normalize_template_text(content)

    # Detect if input is JSON (starts with '{' or '[' after stripping whitespace)
    # JSON input uses safe loading, then is wrapped in CommentedMap/CommentedSeq so
    # ruamel flow-style annotations still work for compact formatting.
    stripped = normalized_content.lstrip()
    is_json = stripped.startswith("{") or stripped.startswith("[")

    if is_json:
        # Use safe loading for JSON to avoid carrying over any YAML-specific styling,
        # then wrap basic containers in ruamel comment-aware types so flow_style works.
        yaml_safe = YAML(typ="safe")
        yaml_safe.default_flow_style = False
        data = yaml_safe.load(normalized_content)

        def _wrap_commented(node: Any) -> Any:
            if isinstance(node, dict):
                commented = CommentedMap()
                for key, value in node.items():
                    commented[key] = _wrap_commented(value)
                return commented
            if isinstance(node, list):
                commented_seq = CommentedSeq()
                for item in node:
                    commented_seq.append(_wrap_commented(item))
                return commented_seq
            return node

        data = _wrap_commented(data)
    else:
        # Use round-trip loading for YAML - preserves comments
        yaml_instance = create_cfn_yaml()
        data = yaml_instance.load(normalized_content)

    # Convert to OrderedDict to preserve order
    ordered_data = ensure_cfn_tags(_to_ordered_dict(data))
    combined_titles = _build_resource_title_map(ordered_data, resource_titles)
    ordered_data = convert_stepfunction_definitions(ordered_data)
    ordered_data = convert_joins_to_sub(ordered_data)
    _mark_literal_blocks(ordered_data)

    # Apply compact flow style if requested
    if flow_style == "compact":
        _mark_compact_flow_style(ordered_data)

    # Dump with ruamel.yaml
    stream = io.StringIO()
    dump_yaml = create_cfn_yaml()
    dump_yaml.dump(ordered_data, stream)
    formatted = stream.getvalue()

    # Post-process to adjust intrinsic list indentation and align values
    formatted_lines = formatted.splitlines()
    formatted_lines = _fix_nested_cfntag_indent(formatted_lines)
    formatted_lines = _align_values(formatted_lines, alignment_column)
    formatted_lines = _align_block_literals(formatted_lines, alignment_column)
    formatted_lines = _format_literal_blocks(formatted_lines, alignment_column)

    # Add proper spacing and comments
    formatted_lines = _add_section_spacing(
        formatted_lines,
        alignment_column=alignment_column,
        resource_titles=combined_titles,
    )

    return "\n".join(formatted_lines)


def _add_section_spacing(
    lines: List[str],
    alignment_column: int = 40,
    resource_titles: Optional[Dict[str, str]] = None,
) -> List[str]:
    """Add spacing between major sections for better readability."""
    result: List[str] = []

    top_level_keys = {
        "AWSTemplateFormatVersion:",
        "Description:",
        "Metadata:",
        "Parameters:",
        "Mappings:",
        "Conditions:",
        "Transform:",
        "Resources:",
        "Outputs:",
    }
    # Treat these as a compact "preamble" block that should not be split
    # by extra blank lines between each other.
    preamble_keys = {
        "AWSTemplateFormatVersion:",
        "Description:",
        "Transform:",
    }

    in_resources = False
    last_top_level_key: Optional[str] = None

    for i, line in enumerate(lines):
        stripped = line.strip()
        # Check if this is a top-level key (must be at zero indentation)
        is_top_level = not line.startswith(" ") and any(
            stripped.startswith(key) for key in top_level_keys
        )

        current_top_level_key: Optional[str] = None
        if is_top_level:
            for key in top_level_keys:
                if stripped.startswith(key):
                    current_top_level_key = key
                    break

        # Update section flags
        if stripped == "Resources:":
            in_resources = True
        elif stripped == "Outputs:":
            in_resources = False
        elif is_top_level:
            in_resources = False

        # Add spacing before top-level sections (except the first one)
        if is_top_level and i > 0:
            # Keep the header/preamble block (template version, description,
            # transform) compact by not inserting extra blank lines between
            # those keys. For all other transitions, retain the double-blank
            # separation.
            if not (current_top_level_key in preamble_keys and last_top_level_key in preamble_keys):
                # Count existing trailing blank lines in result
                trailing_blanks = 0
                for j in range(len(result) - 1, -1, -1):
                    if result[j] == "":
                        trailing_blanks += 1
                    else:
                        break
                # Add blank lines only if needed to reach 2
                for _ in range(max(0, 2 - trailing_blanks)):
                    result.append("")

        if is_top_level:
            last_top_level_key = current_top_level_key

        # For Resources section ONLY, add comments for each resource.
        # Resources are identified by having exactly 2 spaces indentation and being in Resources section.
        if in_resources and not is_top_level:
            # Check if this is a resource definition (2 spaces of indentation, contains colon)
            if line.startswith("  ") and not line.startswith("    ") and ":" in line:
                # This is potentially a resource definition
                # Additional check: make sure it's not a Properties key or other sub-key
                key_name = line.strip().rstrip(":")
                display_name = None
                if resource_titles is not None and key_name in resource_titles:
                    display_name = resource_titles[key_name]
                elif resource_titles is None and key_name and key_name[0].isupper():
                    display_name = key_name
                if display_name:
                    # If there is already a user comment immediately preceding this
                    # resource, treat that as the header and skip inserting our own.
                    header_already_exists = False
                    # Look back to the most recent non-empty line; if it is a comment,
                    # assume the user has provided a header/comment block.
                    for j in range(len(result) - 1, -1, -1):
                        prev = result[j]
                        if not prev:
                            continue
                        if prev.lstrip().startswith("#"):
                            header_already_exists = True
                        break

                    # As a safety net, still honour our previous behaviour of detecting
                    # an exact Pretty CFN header line for this resource.
                    if not header_already_exists:
                        expected_comment = f"  ## {display_name}"
                        for j in range(len(result) - 1, max(0, len(result) - 6), -1):
                            if result[j] == expected_comment:
                                header_already_exists = True
                                break

                    if not header_already_exists:
                        header_lines = _render_resource_header(display_name)
                        result.extend(header_lines)

        result.append(line)

    return result


def _render_resource_header(name: str) -> List[str]:
    """Render a simple block header for a resource."""
    return ["", "  ##", f"  ## {name}", "  ##", ""]


def _collect_resource_titles(data: Any) -> Dict[str, str]:
    titles: Dict[str, str] = {}
    resources = data.get("Resources") if isinstance(data, dict) else None
    if not isinstance(resources, (dict, OrderedDict)):
        return titles
    for logical_id, body in resources.items():
        if not isinstance(body, dict):
            continue
        metadata = body.get("Metadata")
        if isinstance(metadata, dict):
            path = metadata.get("aws:cdk:path")
            if isinstance(path, str) and path:
                cleaned = path.strip().replace("/", " / ")
                # Collapse multiple spaces that may result from leading slashes
                cleaned = " ".join(cleaned.split())
                titles[logical_id] = cleaned
    return titles


def _build_resource_title_map(
    data: Any, provided_titles: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    titles: Dict[str, str] = {}
    resources = data.get("Resources") if isinstance(data, dict) else None
    provided = provided_titles or {}
    if not isinstance(resources, (dict, OrderedDict)):
        return titles
    for logical_id, body in resources.items():
        if logical_id in provided:
            titles[logical_id] = provided[logical_id]
            continue
        if isinstance(body, (dict, OrderedDict)):
            metadata = body.get("Metadata")
            if isinstance(metadata, (dict, OrderedDict)):
                path = metadata.get("aws:cdk:path")
                if isinstance(path, str) and path:
                    cleaned = path.strip().replace("/", " / ")
                    cleaned = " ".join(cleaned.split())
                    titles[logical_id] = cleaned
                    continue
        titles[logical_id] = logical_id
    return titles


def format_cfn_file(
    input_path: str,
    output_path: Optional[str] = None,
    alignment_column: int = 40,
    flow_style: str = "block",
) -> None:
    """
    Format a CloudFormation YAML file.

    Args:
        input_path: Path to the input YAML file
        output_path: Path to the output file (if None, overwrites input)
        alignment_column: The column position to align values to
    """
    with open(input_path, "r") as f:
        content = f.read()

    formatted = format_cfn_yaml(
        content,
        alignment_column,
        flow_style=flow_style,
    )

    output = output_path or input_path
    with open(output, "w") as f:
        f.write(formatted)

    print(f"Formatted template written to {output}")
