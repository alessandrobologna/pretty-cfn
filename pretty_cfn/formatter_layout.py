"""Internal helpers for layout and alignment of formatted YAML output."""

from __future__ import annotations

from collections import OrderedDict
from typing import Any, List, Optional, Tuple

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq

from .formatter_intrinsics import CFNTag, LiteralStr


def _is_simple_scalar(value: Any) -> bool:
    """Return True if value is a flat scalar suitable for inline flow style."""

    if isinstance(value, CFNTag):
        inner = value.value
        if isinstance(inner, (dict, list, OrderedDict, CFNTag)):
            return False
        if isinstance(inner, str) and "\n" in inner:
            return False
        return True
    if isinstance(value, LiteralStr):
        return False
    if isinstance(value, str):
        return "\n" not in value
    if isinstance(value, (int, float, bool)) or value is None:
        return True
    return False


def _should_use_compact_flow_for_mapping(data: Any) -> bool:
    """Decide whether a mapping should be rendered in compact flow style."""

    if not isinstance(data, (dict, OrderedDict, CommentedMap)):
        return False
    # Zero or one element: safe to inline when scalar.
    if len(data) == 0:
        return False
    if len(data) > 2:
        return False
    if not all(_is_simple_scalar(v) for v in data.values()):
        return False
    # Don't use flow style if there are comments attached - they would be mangled
    if isinstance(data, CommentedMap) and hasattr(data, "ca"):
        if data.ca.comment or data.ca.items:
            return False

    # Rough length check to avoid very long lines.
    parts: list[str] = []
    for key, value in data.items():
        key_str = str(key)
        if isinstance(value, CFNTag):
            val_str = f"!{value.tag} {value.value}"
        else:
            val_str = str(value)
        parts.append(f"{key_str}: {val_str}")
    inner = ", ".join(parts)
    estimated_len = len("{ " + inner + " }")
    return estimated_len <= 80


def _should_use_compact_flow_for_sequence(data: Any) -> bool:
    """Decide whether a sequence should be rendered in compact flow style."""

    if not isinstance(data, (list, CommentedSeq)):
        return False
    if len(data) == 0:
        return False
    if len(data) > 3:
        return False
    if not all(_is_simple_scalar(item) for item in data):
        return False
    # Don't use flow style if there are comments attached - they would be mangled
    if isinstance(data, CommentedSeq) and hasattr(data, "ca"):
        if data.ca.comment or data.ca.items:
            return False

    # Estimate the length of the inline sequence
    parts: list[str] = []
    for item in data:
        if isinstance(item, CFNTag):
            val_str = f"!{item.tag} {item.value}"
        else:
            val_str = str(item)
        parts.append(val_str)
    inner = ", ".join(parts)
    estimated_len = len("[ " + inner + " ]")
    return estimated_len <= 80


def _mark_compact_flow_style(node: Any) -> None:
    """Mark eligible small mappings and sequences for compact flow style."""

    if isinstance(node, CommentedMap):
        for key, value in node.items():
            _mark_compact_flow_style(value)
        if _should_use_compact_flow_for_mapping(node):
            node.fa.set_flow_style()
        return

    if isinstance(node, CommentedSeq):
        for item in node:
            _mark_compact_flow_style(item)
        if _should_use_compact_flow_for_sequence(node):
            node.fa.set_flow_style()
        return

    if isinstance(node, dict):
        for value in node.values():
            _mark_compact_flow_style(value)
        return

    if isinstance(node, list):
        for item in node:
            _mark_compact_flow_style(item)
        return


def _find_kv_colon_idx(line: str) -> Optional[int]:
    """Return the index of the colon that separates key and value."""
    in_squote = False
    in_dquote = False
    brace_stack: List[str] = []
    i = 0
    while i < len(line):
        ch = line[i]
        if ch == "'" and not in_dquote:
            in_squote = not in_squote
        elif ch == '"' and not in_squote:
            in_dquote = not in_dquote
        elif not in_squote and not in_dquote:
            if ch in "{[(":
                brace_stack.append(ch)
            elif ch in "}])" and brace_stack:
                brace_stack.pop()
            elif ch == ":" and not brace_stack:
                nxt = line[i + 1] if i + 1 < len(line) else ""
                if nxt in (" ", "\n", ""):
                    return i
        i += 1
    return None


def _align_values(lines: List[str], alignment_column: int) -> List[str]:
    """Align key/value separators at the requested column."""

    aligned_lines: List[str] = []
    literal_block_indent = None

    i = 0
    while i < len(lines):
        line = lines[i]
        if not line.strip():
            aligned_lines.append(line)
            i += 1
            continue

        stripped = line.lstrip(" ")
        indent = len(line) - len(stripped)

        # If we are inside a literal block, keep indentation untouched
        if literal_block_indent is not None:
            if indent > literal_block_indent:
                aligned_lines.append(line)
                i += 1
                continue
            # We've exited the literal block (indent <= literal_block_indent)
            literal_block_indent = None

        if ":" in stripped and not stripped.startswith("- "):
            colon_idx = _find_kv_colon_idx(stripped)
            if colon_idx is not None:
                key_part = stripped[: colon_idx + 1]
                value_part = stripped[colon_idx + 1 :].lstrip()

                indent_str = " " * indent

                if value_part and not value_part.startswith("\n"):
                    current_length = len(key_part)
                    padding_needed = max(1, alignment_column - indent - current_length)
                    aligned_line = indent_str + key_part + " " * padding_needed + value_part
                    aligned_lines.append(aligned_line)

                    value_stripped = value_part.rstrip()
                    if value_stripped and value_stripped[0] in ("|", ">"):
                        literal_block_indent = indent
                else:
                    aligned_lines.append(line)
            else:
                aligned_lines.append(line)
        elif stripped.startswith("-"):
            indent_str = " " * indent
            after_dash = stripped[1:]
            value_part = after_dash.lstrip()
            if not value_part:
                aligned_lines.append(line)
                i += 1
                continue

            if value_part.startswith("- "):
                nested_value = value_part[2:].lstrip()
                if nested_value:
                    nested_colon_idx = (
                        _find_kv_colon_idx(nested_value) if ":" in nested_value else None
                    )
                    if nested_colon_idx is not None:
                        aligned_lines.append(f"{indent_str}- - {nested_value}")
                    else:
                        padding_needed = max(1, alignment_column - (indent + 3))
                        aligned_line = f"{indent_str}- -{' ' * padding_needed}{nested_value}"
                        aligned_lines.append(aligned_line)
                else:
                    aligned_lines.append(f"{indent_str}- -")
                i += 1
                continue

            colon_idx = _find_kv_colon_idx(value_part) if ":" in value_part else None
            if colon_idx is not None:
                inner_key_part = value_part[: colon_idx + 1]
                inner_value_part = value_part[colon_idx + 1 :].lstrip()
                inner_key = inner_key_part.lstrip()
                if inner_value_part:
                    padding_needed = max(1, alignment_column - (indent + 2) - len(inner_key))
                    aligned_line = (
                        f"{indent_str}- {inner_key}" + " " * padding_needed + inner_value_part
                    )
                else:
                    aligned_line = f"{indent_str}- {inner_key}"
                aligned_lines.append(aligned_line)
                i += 1
                continue

            padding_needed = max(1, alignment_column - indent - 1)
            aligned_line = f"{indent_str}-{' ' * padding_needed}{value_part}"
            aligned_lines.append(aligned_line)
        else:
            aligned_lines.append(line)

        i += 1

    return aligned_lines


def _align_block_literals(lines: List[str], alignment_column: int) -> List[str]:
    """Indent literal block content so that visible characters align with the value column."""

    result: List[str] = []
    i = 0
    total = len(lines)

    while i < total:
        line = lines[i]
        result.append(line)

        value_part = None
        if ":" in line:
            value_part = line.split(":", 1)[1].strip()
        if value_part and value_part.startswith("|"):
            base_indent = len(line) - len(line.lstrip(" "))
            target_base = max(alignment_column, base_indent + 2)
            block_lines: List[str] = []
            j = i + 1
            while j < total:
                candidate = lines[j]
                if candidate.strip() == "":
                    k = j + 1
                    while k < total and lines[k].strip() == "":
                        k += 1
                    if k < total:
                        next_indent = len(lines[k]) - len(lines[k].lstrip(" "))
                        if next_indent > base_indent:
                            block_lines.append("")
                            j += 1
                            continue
                    break
                indent = len(candidate) - len(candidate.lstrip(" "))
                if indent <= base_indent:
                    break
                block_lines.append(candidate)
                j += 1

            if block_lines:
                min_indent = None
                for blk in block_lines:
                    if not blk:
                        continue
                    indent = len(blk) - len(blk.lstrip(" "))
                    if min_indent is None or indent < min_indent:
                        min_indent = indent
                if min_indent is None:
                    min_indent = base_indent + 2

                for blk in block_lines:
                    if not blk:
                        result.append(" " * target_base)
                        continue
                    indent = len(blk) - len(blk.lstrip(" "))
                    relative = max(0, indent - min_indent)
                    content = blk.lstrip(" ")
                    result.append(" " * (target_base + relative) + content)
            i = j
            continue

        i += 1

    return result


def _format_literal_blocks(
    lines: List[str],
    alignment_column: int,
    property_names: Tuple[str, ...] = ("InlineCode", "Definition"),
) -> List[str]:
    """Rewrite selected string properties with escaped newlines into literal blocks."""

    result: List[str] = []
    i = 0
    total = len(lines)
    while i < total:
        line = lines[i]
        stripped = line.lstrip()
        matches_property = next(
            (name for name in property_names if stripped.startswith(f"{name}:")), None
        )
        if not matches_property:
            result.append(line)
            i += 1
            continue
        colon_idx = line.find(":")
        if colon_idx == -1:
            result.append(line)
            i += 1
            continue
        decoded, consumed = _extract_quoted_scalar(lines, i, colon_idx)
        if decoded is None or "\n" not in decoded:
            result.extend(lines[i : i + consumed])
            i += consumed
            continue

        header = line[: colon_idx + 1]
        base_indent = len(line) - len(line.lstrip(" "))
        target_base = max(alignment_column, base_indent + 2)
        padding = " " * max(1, target_base - colon_idx - 1)
        result.append(f"{header}{padding}|-")
        for raw_line in decoded.split("\n"):
            if raw_line == "":
                result.append(" " * target_base)
            else:
                result.append(" " * target_base + raw_line)
        i += consumed

    return result


def _is_escaped(value: str) -> bool:
    if not value:
        return False
    backslashes = 0
    idx = len(value) - 1
    while idx >= 0 and value[idx] == "\\":
        backslashes += 1
        idx -= 1
    return backslashes % 2 == 1


def _extract_quoted_scalar(
    lines: List[str], start_idx: int, colon_idx: int
) -> Tuple[Optional[str], int]:
    value_fragment = lines[start_idx][colon_idx + 1 :]
    if not value_fragment.lstrip().startswith('"'):
        return None, 1

    snippet_lines = [f"value:{value_fragment}"]
    consumed = 1

    while True:
        stripped = snippet_lines[-1].rstrip()
        if stripped.endswith('"') and not _is_escaped(stripped):
            break
        if start_idx + consumed >= len(lines):
            return None, consumed
        snippet_lines.append(lines[start_idx + consumed])
        consumed += 1

    snippet = "\n".join(snippet_lines) + "\n"
    try:
        safe_yaml = YAML(typ="safe")
        parsed = safe_yaml.load(snippet)
    except Exception:
        return None, consumed
    if isinstance(parsed, dict):
        value = parsed.get("value")
        if isinstance(value, str):
            return value, consumed
    return None, consumed


_CFNTAG_LIST_INTRINSICS = frozenset(
    [
        "!And",
        "!Or",
        "!Not",
        "!Equals",
        "!If",
        "!Condition",
        "!FindInMap",
        "!Select",
        "!Split",
        "!Cidr",
        "!Contains",
        "!EachMemberEquals",
        "!EachMemberIn",
    ]
)


def _fix_nested_cfntag_indent(lines: List[str]) -> List[str]:
    """Fix indentation of nested CFNTag sequences."""

    result: List[str] = []
    cfntag_contexts: List[int] = []

    for line in lines:
        if not line.strip():
            result.append(line)
            continue

        stripped = line.lstrip()
        current_indent = len(line) - len(stripped)

        while cfntag_contexts and current_indent <= cfntag_contexts[-1]:
            cfntag_contexts.pop()

        if stripped.startswith("-"):
            after_dash = stripped[1:].lstrip()
            has_cfntag_list = any(after_dash.startswith(tag) for tag in _CFNTAG_LIST_INTRINSICS)

            if cfntag_contexts:
                parent_indent = cfntag_contexts[-1]
                expected_indent = parent_indent + 2
                if current_indent > expected_indent:
                    line = " " * expected_indent + stripped
                    current_indent = expected_indent

            if has_cfntag_list:
                cfntag_contexts.append(current_indent)

        result.append(line)

    return result
