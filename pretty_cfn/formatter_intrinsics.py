"""Internal helpers for CFN/YAML intrinsics and structural transforms.

This module centralizes:
- CFNTag / LiteralStr types and YAML configuration
- JSON-style intrinsic conversion helpers
- Join/Sub normalization
- StepFunctions DefinitionString -> Definition conversion
"""

from __future__ import annotations

import json
import re
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple

import ruamel.yaml
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from ruamel.yaml.scalarstring import LiteralScalarString


# CloudFormation intrinsic function tags
CFN_TAGS = [
    "Ref",
    "GetAtt",
    "Base64",
    "GetAZs",
    "ImportValue",
    "Join",
    "Select",
    "Split",
    "Sub",
    "FindInMap",
    "If",
    "Not",
    "Equals",
    "And",
    "Or",
    "Cidr",
    "Transform",
    "Contains",
]


class CFNTag:
    """Class to represent CloudFormation intrinsic function tags."""

    def __init__(self, tag, value):
        self.tag = tag
        self.value = value

    def __repr__(self):
        return f"{self.tag}({self.value})"

    def __eq__(self, other):
        if not isinstance(other, CFNTag):
            return False
        return self.tag == other.tag and self.value == other.value

    def __hash__(self):
        # Make CFNTag hashable for use in sets/dicts if needed
        return hash((self.tag, str(self.value)))


def _cfn_tag_constructor(loader, node):
    """Constructor for CloudFormation intrinsic function tags (ruamel.yaml)."""
    tag = node.tag.lstrip("!")
    if isinstance(node, ruamel.yaml.ScalarNode):
        value = loader.construct_scalar(node)
    elif isinstance(node, ruamel.yaml.SequenceNode):
        value = loader.construct_sequence(node, deep=True)
    elif isinstance(node, ruamel.yaml.MappingNode):
        value = loader.construct_mapping(node, deep=True)
    else:
        value = None
    return CFNTag(tag, value)


def _cfn_tag_representer(dumper, data):
    """Representer for CloudFormation intrinsic function tags (ruamel.yaml)."""
    tag = f"!{data.tag}"
    value = data.value

    # Special handling for GetAtt - convert list to dotted string
    if data.tag == "GetAtt":
        scalar = _stringify_getatt_value(value)
        if scalar is not None:
            return dumper.represent_scalar(tag, scalar)

    if isinstance(value, (dict, OrderedDict, CommentedMap)):
        return dumper.represent_mapping(tag, value.items())
    elif isinstance(value, (list, CommentedSeq)):
        return dumper.represent_sequence(tag, value)
    elif isinstance(value, CFNTag):
        # Nested intrinsic - expand to mapping form
        key = f"Fn::{value.tag}"
        return dumper.represent_mapping(tag, [(key, value.value)])
    else:
        return dumper.represent_scalar(tag, str(value) if value is not None else "")


def _none_representer(dumper, data):
    """Represent None as empty string."""
    return dumper.represent_scalar("tag:yaml.org,2002:null", "")


def _normalize_multiline_block(value: str) -> str:
    """Trim leading blank lines and common indentation from multi-line scalars."""

    if not value:
        return value

    normalized = value.replace("\r\n", "\n")

    # Drop leading blank lines which only add vertical space
    while normalized.startswith("\n"):
        normalized = normalized[1:]

    if not normalized:
        return ""

    lines = normalized.split("\n")
    # Determine minimum indentation among non-empty lines
    indent = None
    for line in lines:
        if not line.strip():
            continue
        leading_spaces = len(line) - len(line.lstrip(" "))
        if indent is None or leading_spaces < indent:
            indent = leading_spaces

    if indent and indent > 0:
        trimmed_lines = [line[indent:] if len(line) >= indent else line for line in lines]
        normalized = "\n".join(trimmed_lines)

    return normalized


def _str_representer(dumper, data):
    """Custom string representer with quoting rules."""
    # Multi-line strings use block style
    if "\n" in data:
        normalized = _normalize_multiline_block(data)
        return dumper.represent_scalar("tag:yaml.org,2002:str", normalized, style="|")

    # Quote boolean-like strings
    if data in ("true", "false", "null", "True", "False", "Null", "TRUE", "FALSE", "NULL"):
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style='"')

    # Quote strings that need it
    if _needs_quotes(data):
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style='"')

    # Plain style for regular strings
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style=None)


def _needs_quotes(s: str) -> bool:
    """Check if a string needs quotes."""
    if not s:
        return False
    # CloudFormation tags don't need quotes
    if s.startswith("!"):
        return False
    # Numbers that should be strings
    if re.match(r"^-?\d+(\.\d+)?$", s):
        return True
    # YAML special values
    if s in ("yes", "no", "on", "off"):
        return True
    # Strings with special characters at start
    if s[0] in ("*", "&", "%", "@", "`", "|", ">", "{", "[", "'", '"'):
        return True
    return False


def create_cfn_yaml(*, preserve_quotes: bool = False, width: int = 200) -> YAML:
    """Create a configured ruamel.yaml instance for CloudFormation templates."""
    yaml = YAML()
    yaml.preserve_quotes = preserve_quotes
    yaml.default_flow_style = False
    yaml.width = width
    # Set indentation: mapping=2, sequence=4 spaces, offset=2
    yaml.indent(mapping=2, sequence=4, offset=2)

    # Register constructors for CloudFormation tags
    for tag in CFN_TAGS:
        yaml.Constructor.add_constructor(f"!{tag}", _cfn_tag_constructor)

    # Register representers
    yaml.Representer.add_representer(CFNTag, _cfn_tag_representer)
    yaml.Representer.add_representer(type(None), _none_representer)
    yaml.Representer.add_representer(str, _str_representer)
    yaml.Representer.add_representer(OrderedDict, yaml.Representer.represent_dict)

    return yaml


class CFNLoader:
    """Loader wrapper for backward compatibility."""

    def __init__(self, content: str):
        self._content = content
        self._yaml = create_cfn_yaml()

    def get_single_data(self):
        """Load and return the YAML data."""
        return self._yaml.load(self._content)


JSON_INTRINSIC_MAP: Dict[str, str] = {
    "Ref": "Ref",
    "Fn::GetAtt": "GetAtt",
    "Fn::Base64": "Base64",
    "Fn::GetAZs": "GetAZs",
    "Fn::ImportValue": "ImportValue",
    "Fn::Join": "Join",
    "Fn::Select": "Select",
    "Fn::Split": "Split",
    "Fn::Sub": "Sub",
    "Fn::FindInMap": "FindInMap",
    "Fn::If": "If",
    "Fn::Not": "Not",
    "Fn::Equals": "Equals",
    "Fn::And": "And",
    "Fn::Or": "Or",
    "Fn::Cidr": "Cidr",
    "Fn::Transform": "Transform",
    "Fn::Contains": "Contains",
    "Fn::EachMemberEquals": "EachMemberEquals",
    "Fn::EachMemberIn": "EachMemberIn",
    "Fn::RefAll": "RefAll",
    "Fn::ValueOf": "ValueOf",
    "Fn::ValueOfAll": "ValueOfAll",
}


# Alias for ruamel.yaml's literal scalar string type
LiteralStr = LiteralScalarString


def normalize_template_text(content: str) -> str:
    """Normalize newlines without mutating intrinsic syntax."""

    if "\r" not in content:
        return content
    return content.replace("\r\n", "\n").replace("\r", "\n")


def _stringify_getatt_value(value: Any) -> Optional[str]:
    if isinstance(value, str):
        return value
    if isinstance(value, list) and value and all(isinstance(part, str) for part in value):
        head, *rest = value
        tail = ".".join(rest)
        return f"{head}.{tail}" if tail else head
    return None


def _copy_comment_attr(src: CommentedMap, dst: CommentedMap, key: str) -> None:
    """Copy comment attributes for a key from source to destination CommentedMap."""
    if hasattr(src, "ca") and src.ca.items and key in src.ca.items:
        if not hasattr(dst, "ca"):
            return
        dst.ca.items[key] = src.ca.items[key]


def _to_ordered_dict(obj: Any, *, _preserve_comments: bool = True) -> Any:
    """Recursively reorder dictionaries while preserving comments."""
    if isinstance(obj, CFNTag):
        # Preserve CloudFormation tags
        return CFNTag(obj.tag, _to_ordered_dict(obj.value, _preserve_comments=_preserve_comments))
    if isinstance(obj, dict):
        # Define the preferred order for top-level keys
        top_level_order = [
            "AWSTemplateFormatVersion",
            "Description",
            "Metadata",
            "Parameters",
            "Mappings",
            "Conditions",
            "Transform",
            "Resources",
            "Outputs",
            "Rules",
        ]

        # Define the preferred order for resource properties
        resource_order = [
            "Type",
            "Properties",
            "DependsOn",
            "Condition",
            "CreationPolicy",
            "DeletionPolicy",
            "UpdatePolicy",
            "UpdateReplacePolicy",
            "Metadata",
        ]

        # Define the preferred order for parameter properties
        parameter_order = [
            "Type",
            "Description",
            "Default",
            "AllowedValues",
            "AllowedPattern",
            "MinLength",
            "MaxLength",
            "MinValue",
            "MaxValue",
            "NoEcho",
            "ConstraintDescription",
        ]

        # Preserve CommentedMap type if input is CommentedMap
        is_commented = _preserve_comments and isinstance(obj, CommentedMap)
        ordered: dict = CommentedMap() if is_commented else OrderedDict()

        # Copy top-level comment (comment before first key)
        if is_commented and hasattr(obj, "ca") and obj.ca.comment:
            ordered.ca.comment = obj.ca.comment

        def _add_key(key: str) -> None:
            """Add a key with its value and preserve comments."""
            ordered[key] = _to_ordered_dict(obj[key], _preserve_comments=_preserve_comments)
            if is_commented:
                _copy_comment_attr(obj, ordered, key)

        # Check if this is the top level
        if "AWSTemplateFormatVersion" in obj or "Resources" in obj:
            # Top level - use top level ordering
            for key in top_level_order:
                if key in obj:
                    _add_key(key)
            # Add any remaining keys
            for key in obj:
                if key not in ordered:
                    _add_key(key)
        else:
            # Check if this is a resource definition
            if (
                "Type" in obj
                and isinstance(obj.get("Type"), str)
                and obj["Type"].startswith("AWS::")
            ):
                # Resource definition - use resource ordering
                for key in resource_order:
                    if key in obj:
                        _add_key(key)
                # Add any remaining keys
                for key in obj:
                    if key not in ordered:
                        _add_key(key)
            # Check if this is a parameter definition
            elif (
                "Type" in obj
                and isinstance(obj.get("Type"), str)
                and obj["Type"]
                in [
                    "String",
                    "Number",
                    "CommaDelimitedList",
                    "AWS::EC2::VPC::Id",
                    "AWS::EC2::Subnet::Id",
                    "List<AWS::EC2::Subnet::Id>",
                    "AWS::EC2::SecurityGroup::Id",
                    "AWS::SSM::Parameter::Value<String>",
                ]
            ):
                # Parameter definition - use parameter ordering
                for key in parameter_order:
                    if key in obj:
                        _add_key(key)
                # Add any remaining keys
                for key in obj:
                    if key not in ordered:
                        _add_key(key)
            else:
                # Regular dict - preserve original order
                for key in obj:
                    _add_key(key)

        return ordered
    if isinstance(obj, list):
        # Preserve CommentedSeq type if input is CommentedSeq
        if _preserve_comments and isinstance(obj, CommentedSeq):
            result = CommentedSeq(
                [_to_ordered_dict(item, _preserve_comments=_preserve_comments) for item in obj]
            )
            # Copy list-level comments
            if hasattr(obj, "ca"):
                if obj.ca.comment:
                    result.ca.comment = obj.ca.comment
                # Copy item-level comments (e.g., comments before list elements)
                if obj.ca.items:
                    for idx, comment_tuple in obj.ca.items.items():
                        result.ca.items[idx] = comment_tuple
            return result
        return [_to_ordered_dict(item, _preserve_comments=_preserve_comments) for item in obj]
    return obj


def ensure_cfn_tags(obj: Any, *, _preserve_comments: bool = True) -> Any:
    """Convert JSON-style intrinsic dictionaries into CFNTag instances."""

    if isinstance(obj, CFNTag):
        return (
            CFNTag(obj.tag, ensure_cfn_tags(obj.value, _preserve_comments=_preserve_comments))
            if isinstance(obj.value, (dict, list, CFNTag))
            else obj
        )

    if isinstance(obj, dict):
        if len(obj) == 1:
            key, value = next(iter(obj.items()))
            tag = JSON_INTRINSIC_MAP.get(key)
            if tag:
                return CFNTag(tag, ensure_cfn_tags(value, _preserve_comments=_preserve_comments))

        # Preserve CommentedMap type if input is CommentedMap
        is_commented = _preserve_comments and isinstance(obj, CommentedMap)
        ordered: dict = CommentedMap() if is_commented else OrderedDict()

        # Copy top-level comment
        if is_commented and hasattr(obj, "ca") and obj.ca.comment:
            ordered.ca.comment = obj.ca.comment

        for key, value in obj.items():
            ordered[key] = ensure_cfn_tags(value, _preserve_comments=_preserve_comments)
            if is_commented:
                _copy_comment_attr(obj, ordered, key)

        return ordered

    if isinstance(obj, list):
        # Preserve CommentedSeq type if input is CommentedSeq
        if _preserve_comments and isinstance(obj, CommentedSeq):
            result = CommentedSeq(
                [ensure_cfn_tags(item, _preserve_comments=_preserve_comments) for item in obj]
            )
            if hasattr(obj, "ca"):
                if obj.ca.comment:
                    result.ca.comment = obj.ca.comment
                # Copy item-level comments (e.g., comments before list elements)
                if obj.ca.items:
                    for idx, comment_tuple in obj.ca.items.items():
                        result.ca.items[idx] = comment_tuple
            return result
        return [ensure_cfn_tags(item, _preserve_comments=_preserve_comments) for item in obj]

    return obj


def convert_joins_to_sub(obj: Any) -> Any:
    """Recursively convert Fn::Join nodes to Fn::Sub strings where possible."""

    def recurse(node: Any) -> Any:
        if isinstance(node, CFNTag):
            new_value = (
                recurse(node.value)
                if isinstance(node.value, (list, dict, OrderedDict, CommentedMap, CFNTag))
                else node.value
            )
            if node.tag == "Join":
                converted = _join_tag_to_sub(new_value)
                if converted is not None:
                    return CFNTag("Sub", converted)
            if new_value is not node.value:
                return CFNTag(node.tag, new_value)
            return node
        if isinstance(node, (list, CommentedSeq)):
            changed = False
            new_list = CommentedSeq() if isinstance(node, CommentedSeq) else []
            for item in node:
                new_item = recurse(item)
                if new_item is not item:
                    changed = True
                new_list.append(new_item)
            if changed:
                # Preserve list-level comments
                if isinstance(node, CommentedSeq) and hasattr(node, "ca"):
                    if node.ca.comment:
                        new_list.ca.comment = node.ca.comment
                    # Copy item-level comments
                    if node.ca.items:
                        for idx, comment_tuple in node.ca.items.items():
                            new_list.ca.items[idx] = comment_tuple
                return new_list
            return node
        if isinstance(node, CommentedMap):
            changed = False
            new_dict = CommentedMap()
            for key, value in node.items():
                new_value = recurse(value)
                if new_value is not value:
                    changed = True
                new_dict[key] = new_value
                _copy_comment_attr(node, new_dict, key)
            if changed:
                # Preserve top-level comment
                if hasattr(node, "ca") and node.ca.comment:
                    new_dict.ca.comment = node.ca.comment
                return new_dict
            return node
        if isinstance(node, OrderedDict):
            changed = False
            new_dict_od: OrderedDict[str, Any] = OrderedDict()
            for key, value in node.items():
                new_value = recurse(value)
                if new_value is not value:
                    changed = True
                new_dict_od[key] = new_value
            return new_dict_od if changed else node
        if isinstance(node, dict):
            changed = False
            plain_dict: Dict[str, Any] = {}
            for key, value in node.items():
                new_value = recurse(value)
                if new_value is not value:
                    changed = True
                plain_dict[key] = new_value
            return plain_dict if changed else node
        return node

    return recurse(obj)


def _join_tag_to_sub(value: Any) -> Optional[str]:
    if not isinstance(value, list) or len(value) != 2:
        return None
    delimiter, sequence = value
    if not isinstance(delimiter, str) or not isinstance(sequence, list):
        return None
    parts: List[str] = []
    for idx, item in enumerate(sequence):
        if idx > 0:
            parts.append(delimiter)
        rendered = _render_join_token(item)
        if rendered is None:
            return None
        parts.append(rendered)
    return "".join(parts)


def _render_join_token(token: Any) -> Optional[str]:
    if isinstance(token, CFNTag):
        if token.tag == "Ref" and isinstance(token.value, str):
            return f"${{{token.value}}}"
        if token.tag == "GetAtt":
            if isinstance(token.value, str):
                return f"${{{token.value}}}"
            if isinstance(token.value, list) and len(token.value) >= 2:
                primary = token.value[0]
                attribute = token.value[1]
                if isinstance(primary, str) and isinstance(attribute, str):
                    return f"${{{primary}.{attribute}}}"
    if isinstance(token, str):
        return token
    if isinstance(token, (int, float)):
        return str(token)
    return None


PlaceholderMap = Dict[str, Tuple[Any, Optional[str]]]


def convert_stepfunction_definitions(obj: Any) -> Any:
    """Convert AWS::StepFunctions::StateMachine DefinitionString payloads into Definition maps."""

    if not isinstance(obj, (dict, OrderedDict, CommentedMap)):
        return obj

    resources = obj.get("Resources")
    if not isinstance(resources, (dict, OrderedDict, CommentedMap)):
        return obj

    for _, resource in resources.items():
        if not isinstance(resource, (dict, OrderedDict, CommentedMap)):
            continue
        if resource.get("Type") != "AWS::StepFunctions::StateMachine":
            continue
        properties = resource.get("Properties")
        if not isinstance(properties, (dict, OrderedDict, CommentedMap)):
            continue
        if "Definition" in properties:
            continue
        definition_source = properties.get("DefinitionString")
        if definition_source is None:
            continue
        converted = _convert_definition_string(definition_source)
        if converted is None:
            continue
        properties["Definition"] = converted
        del properties["DefinitionString"]
    return obj


def _convert_definition_string(value: Any) -> Optional[Any]:
    parsed, placeholders = _parse_definition_string(value)
    if parsed is None:
        return None
    substituted = _substitute_placeholders(parsed, placeholders)
    return ensure_cfn_tags(_to_ordered_dict(substituted))


def _parse_definition_string(value: Any) -> Tuple[Optional[Any], PlaceholderMap]:
    """Return the JSON definition payload and placeholder map extracted from DefinitionString."""

    placeholder_map: PlaceholderMap = {}

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None, placeholder_map
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return None, placeholder_map
    elif isinstance(value, CFNTag) and value.tag == "Join":
        parsed = {"Fn::Join": value.value}
    elif isinstance(value, dict) and set(value.keys()) == {"Fn::Join"}:
        parsed = value
    else:
        return None, placeholder_map

    if isinstance(parsed, dict) and set(parsed.keys()) == {"Fn::Join"}:
        join_args = parsed["Fn::Join"]
        json_string, placeholder_map = _render_join_to_string(join_args)
        if json_string is None:
            return None, placeholder_map
        try:
            definition_payload = json.loads(json_string)
        except json.JSONDecodeError:
            return None, placeholder_map
        return definition_payload, placeholder_map

    if isinstance(parsed, (dict, list)):
        return parsed, placeholder_map

    return None, placeholder_map


def _render_join_to_string(join_args: Any) -> Tuple[Optional[str], PlaceholderMap]:
    if not isinstance(join_args, list) or len(join_args) != 2:
        return None, {}
    delimiter, sequence = join_args
    if not isinstance(sequence, list):
        return None, {}
    delim_str = _stringify_join_fragment(delimiter)
    if delim_str is None:
        return None, {}

    placeholder_map: PlaceholderMap = {}
    parts: List[str] = []
    counter = 0
    for idx, item in enumerate(sequence):
        if idx > 0:
            parts.append(delim_str)
        rendered, counter = _stringify_join_part(item, counter, placeholder_map)
        if rendered is None:
            return None, {}
        parts.append(rendered)

    return "".join(parts), placeholder_map


def _stringify_join_fragment(value: Any) -> Optional[str]:
    if value is None:
        return ""
    if isinstance(value, CFNTag):
        # Nested join delimiters are unusual; fall back to string form
        return str(value)
    if isinstance(value, (str, int, float)):
        return str(value)
    if isinstance(value, dict):
        return json.dumps(value)
    return None


def _stringify_join_part(
    item: Any, counter: int, placeholder_map: PlaceholderMap
) -> Tuple[Optional[str], int]:
    if isinstance(item, str):
        return item, counter
    if isinstance(item, (int, float)):
        return str(item), counter
    if isinstance(item, bool):
        return ("true" if item else "false"), counter

    placeholder = f"__PRETTY_CFN_TOKEN_{counter}__"
    inline_text = _inline_substitution_text(item)
    placeholder_map[placeholder] = (item, inline_text)
    counter += 1
    return f"${{{placeholder}}}", counter


def _inline_substitution_text(item: Any) -> Optional[str]:
    if isinstance(item, dict):
        if set(item.keys()) == {"Ref"} and isinstance(item.get("Ref"), str):
            return f"${{{item['Ref']}}}"
        if set(item.keys()) == {"Fn::GetAtt"}:
            target = item.get("Fn::GetAtt")
            if isinstance(target, list) and len(target) >= 2:
                primary, attribute = target[0], target[1]
                if isinstance(primary, str) and isinstance(attribute, str):
                    return f"${{{primary}.{attribute}}}"
            if isinstance(target, str):
                return f"${{{target}}}"
    if isinstance(item, CFNTag):
        if item.tag == "Ref" and isinstance(item.value, str):
            return f"${{{item.value}}}"
        if item.tag == "GetAtt":
            if isinstance(item.value, list) and len(item.value) >= 2:
                primary, attribute = item.value[0], item.value[1]
                if isinstance(primary, str) and isinstance(attribute, str):
                    return f"${{{primary}.{attribute}}}"
            if isinstance(item.value, str):
                return f"${{{item.value}}}"
    return None


def _substitute_placeholders(node: Any, placeholder_map: PlaceholderMap) -> Any:
    placeholder_pattern = re.compile(r"\$\{(__PRETTY_CFN_TOKEN_\d+__)\}")

    def convert_value(name: str) -> Any:
        entry = placeholder_map.get(name)
        if entry is None:
            return f"${{{name}}}"
        original = entry[0]
        if isinstance(original, CFNTag):
            return original
        return ensure_cfn_tags(_to_ordered_dict(original))

    def recurse(current: Any) -> Any:
        if isinstance(current, str):
            matches = list(placeholder_pattern.finditer(current))
            if not matches:
                return current
            if len(matches) == 1 and matches[0].span() == (0, len(current)):
                name = matches[0].group(1)
                return convert_value(name)
            mapping: OrderedDict[str, Any] = OrderedDict()
            new_string_parts: List[str] = []
            last_index = 0
            for match in matches:
                name = match.group(1)
                entry = placeholder_map.get(name)
                inline_text = entry[1] if entry else None
                new_string_parts.append(current[last_index : match.start()])
                if inline_text:
                    new_string_parts.append(inline_text)
                else:
                    placeholder_ref = f"${{{name}}}"
                    new_string_parts.append(placeholder_ref)
                    if name not in mapping:
                        mapping[name] = convert_value(name)
                last_index = match.end()
            new_string_parts.append(current[last_index:])
            rebuilt = "".join(new_string_parts)
            if not mapping:
                return CFNTag("Sub", rebuilt)
            return CFNTag("Sub", [rebuilt, mapping])
        if isinstance(current, list):
            return [recurse(item) for item in current]
        if isinstance(current, dict):
            ordered = OrderedDict()
            for key, value in current.items():
                ordered[key] = recurse(value)
            return ordered
        return current

    return recurse(node)


def _mark_literal_blocks(node: Any) -> None:
    """Recursively convert multiline strings to LiteralStr for block style output."""
    if isinstance(node, LiteralStr):
        return
    if isinstance(node, CFNTag):
        _mark_literal_blocks(node.value)
        return
    if isinstance(node, str):
        return
    if isinstance(node, (list, CommentedSeq)):
        for item in node:
            _mark_literal_blocks(item)
        return
    if isinstance(node, (dict, OrderedDict, CommentedMap)):
        for key, value in list(node.items()):
            # Convert ALL multiline strings to literal block style
            if isinstance(value, str) and "\n" in value and not isinstance(value, LiteralStr):
                node[key] = LiteralStr(value)
            else:
                _mark_literal_blocks(value)
