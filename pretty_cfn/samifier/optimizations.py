"""Higher-level SAM optimizations after core conversions."""

from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
from pathlib import Path
from typing import Iterable, Optional

from .shared import _format_code_uri, _sanitize_path, _ensure_sub_tag


def apply_function_globals(template: dict) -> None:
    resources = template.get("Resources")
    if not isinstance(resources, (dict, OrderedDict)):
        return
    functions = [res for res in resources.values() if _is_serverless_function(res)]
    if len(functions) < 2:
        return

    globals_section = template.setdefault("Globals", OrderedDict())
    function_globals = globals_section.setdefault("Function", OrderedDict())

    for prop in ("Runtime", "MemorySize", "Timeout"):
        common = _shared_value(functions, prop)
        if common is None:
            continue
        function_globals[prop] = common
        for fn in functions:
            fn.get("Properties", {}).pop(prop, None)

    common_env = _shared_environment_variables(functions)
    if common_env:
        env_block = function_globals.setdefault("Environment", OrderedDict())
        env_vars = env_block.setdefault("Variables", OrderedDict())
        for key, value in common_env.items():
            env_vars[key] = value
            for fn in functions:
                properties = fn.get("Properties", {})
                env = properties.get("Environment")
                if not isinstance(env, (dict, OrderedDict)):
                    continue
                vars_block = env.get("Variables")
                if isinstance(vars_block, (dict, OrderedDict)):
                    vars_block.pop(key, None)
                    if not vars_block:
                        env.pop("Variables", None)
                if not env:
                    properties.pop("Environment", None)

    if not function_globals:
        globals_section.pop("Function", None)
    if not globals_section:
        template.pop("Globals", None)


def convert_simple_tables(template: dict) -> bool:
    resources = template.get("Resources")
    if not isinstance(resources, (dict, OrderedDict)):
        return False

    changed = False

    for logical_id, resource in list(resources.items()):
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::DynamoDB::Table":
            continue

        props = resource.get("Properties") or {}
        if not isinstance(props, (dict, OrderedDict)):
            continue

        # Skip if on-demand billing (SimpleTable does not support PAY_PER_REQUEST here)
        billing_mode = props.get("BillingMode")
        provisioned = props.get("ProvisionedThroughput")
        if (
            isinstance(billing_mode, str) and billing_mode.upper() == "PAY_PER_REQUEST"
        ) or not provisioned:
            continue

        # Skip if any unsupported/extra properties are present
        allowed_props = {
            "AttributeDefinitions",
            "KeySchema",
            "ProvisionedThroughput",
            "TableName",
            "Tags",
            "PointInTimeRecoverySpecification",
            "SSESpecification",
            "BillingMode",
        }
        if any(key not in allowed_props for key in props.keys()):
            continue

        key_schema = props.get("KeySchema") or []
        attr_defs = props.get("AttributeDefinitions") or []
        if not (isinstance(key_schema, list) and len(key_schema) == 1):
            continue
        hash_entry = key_schema[0]
        if not isinstance(hash_entry, (dict, OrderedDict)) or hash_entry.get("KeyType") != "HASH":
            continue
        hash_name = hash_entry.get("AttributeName")
        if not isinstance(hash_name, str):
            continue

        attr_lookup = {
            entry.get("AttributeName"): entry.get("AttributeType")
            for entry in attr_defs
            if isinstance(entry, (dict, OrderedDict))
        }
        attr_type = attr_lookup.get(hash_name)
        type_map = {"S": "String", "N": "Number", "B": "Binary"}
        sam_type = type_map.get(attr_type)
        if sam_type is None:
            continue

        new_props = OrderedDict()
        new_props["PrimaryKey"] = {"Name": hash_name, "Type": sam_type}

        if isinstance(provisioned, (dict, OrderedDict)):
            if (
                provisioned.get("ReadCapacityUnits") is not None
                or provisioned.get("WriteCapacityUnits") is not None
            ):
                new_props["ProvisionedThroughput"] = provisioned
        if props.get("TableName") is not None:
            new_props["TableName"] = props["TableName"]
        if props.get("Tags") is not None:
            new_props["Tags"] = props["Tags"]
        if props.get("PointInTimeRecoverySpecification") is not None:
            new_props["PointInTimeRecoverySpecification"] = props[
                "PointInTimeRecoverySpecification"
            ]
        if props.get("SSESpecification") is not None:
            new_props["SSESpecification"] = props["SSESpecification"]

        # Mutate the existing resource so that any comments attached to the
        # logical ID or Type line are preserved across the conversion.
        resource["Type"] = "AWS::Serverless::SimpleTable"
        resource["Properties"] = new_props
        changed = True

    return changed


def convert_layers(
    template: dict,
    *,
    asset_search_paths: Iterable[Path],
    relative_to: Optional[Path],
    asset_stager,
) -> bool:
    resources = template.get("Resources")
    if not isinstance(resources, (dict, OrderedDict)):
        return False

    search_roots = [_sanitize_path(p) for p in asset_search_paths if p is not None]
    changed = False

    for logical_id, resource in list(resources.items()):
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::Lambda::LayerVersion":
            continue

        props = resource.get("Properties") or {}
        if not isinstance(props, (dict, OrderedDict)):
            continue
        content = props.get("Content")
        if not isinstance(content, (dict, OrderedDict)):
            continue

        content_uri = None

        # Prefer local asset metadata (CDK-style aws:asset:path) so we can
        # stage the layer content into the SAM project, matching function
        # asset behavior.
        metadata = resource.get("Metadata") or {}
        asset_path = metadata.get("aws:asset:path")
        asset_property = metadata.get("aws:asset:property")
        if asset_path and (not asset_property or asset_property == "Content"):
            resolved = _resolve_layer_asset(asset_path, search_roots)
            if resolved and resolved.exists():
                if asset_stager:
                    staged = asset_stager.stage_local_path(logical_id, resolved)
                    content_uri = _format_code_uri(staged, relative_to)
                else:
                    content_uri = _format_code_uri(resolved, relative_to)

        # Fall back to S3Bucket/S3Key if no local asset was staged. This path
        # supports both literal strings and intrinsic functions, and will
        # opportunistically download the layer from S3 when an asset stager
        # and AWS environment are available.
        if content_uri is None:
            bucket = content.get("S3Bucket")
            key = content.get("S3Key")
            version = content.get("S3ObjectVersion")
            if bucket is not None and key is not None:
                resolved_bucket = None
                resolved_key = None
                if asset_stager is not None:
                    resolved_bucket = asset_stager.resolve_string(_ensure_sub_tag(bucket))
                    resolved_key = asset_stager.resolve_string(_ensure_sub_tag(key))
                if resolved_bucket is None and isinstance(bucket, str):
                    resolved_bucket = bucket
                if resolved_key is None and isinstance(key, str):
                    resolved_key = key

                if asset_stager is not None and resolved_bucket and resolved_key:
                    try:
                        staged_dir = asset_stager.stage_s3_code(
                            logical_id, resolved_bucket, resolved_key, version
                        )
                        content_uri = _format_code_uri(staged_dir, relative_to)
                    except Exception:
                        # Fall back to a plain S3 URI if staging fails for any reason.
                        pass

                if content_uri is None:
                    uri_obj = OrderedDict()
                    uri_obj["Bucket"] = bucket
                    uri_obj["Key"] = key
                    if version is not None:
                        uri_obj["Version"] = version
                    content_uri = uri_obj

        if content_uri is None:
            continue

        new_props = OrderedDict()
        new_props["ContentUri"] = content_uri
        for key in (
            "Description",
            "LayerName",
            "CompatibleRuntimes",
            "LicenseInfo",
            "RetentionPolicy",
            "CompatibleArchitectures",
        ):
            if key in props:
                new_props[key] = props[key]

        # Mutate in place so any comments on the original Layer resource are
        # preserved when converting to a SAM LayerVersion.
        resource["Type"] = "AWS::Serverless::LayerVersion"
        resource["Properties"] = new_props
        changed = True

    return changed


def _resolve_layer_asset(asset_path: str, search_roots: Iterable[Path]) -> Optional[Path]:
    candidate = Path(asset_path)
    candidates = []
    if candidate.is_absolute():
        candidates.append(candidate)
    else:
        for root in search_roots:
            candidates.append(root / candidate)
        candidates.append(candidate)

    for entry in candidates:
        try:
            if entry.exists():
                return entry.resolve()
        except OSError:
            continue
    if candidates:
        try:
            return candidates[0].resolve()
        except OSError:
            return candidates[0]
    return None


def strip_cdk_metadata(template: dict) -> None:
    _strip_metadata(template)
    parameters = template.get("Parameters")
    if isinstance(parameters, (dict, OrderedDict)) and "BootstrapVersion" in parameters:
        parameters.pop("BootstrapVersion")
        if not parameters:
            template.pop("Parameters", None)


def _is_serverless_function(resource) -> bool:
    return (
        isinstance(resource, (dict, OrderedDict))
        and resource.get("Type") == "AWS::Serverless::Function"
        and isinstance(resource.get("Properties"), (dict, OrderedDict))
    )


def _shared_value(functions, prop: str) -> Optional[object]:
    sentinel = object()
    shared = sentinel
    for fn in functions:
        properties = fn.get("Properties", {})
        if prop not in properties:
            return None
        value = properties[prop]
        if shared is sentinel:
            shared = value
            continue
        if shared != value:
            return None
    return None if shared is sentinel else shared


def _shared_environment_variables(functions) -> Optional[OrderedDict]:
    shared: Optional[OrderedDict] = None
    for fn in functions:
        env = fn.get("Properties", {}).get("Environment")
        variables = env.get("Variables") if isinstance(env, (dict, OrderedDict)) else None
        if not isinstance(variables, (dict, OrderedDict)):
            return None
        if shared is None:
            shared = OrderedDict((k, deepcopy(v)) for k, v in variables.items())
            continue
        for key in list(shared.keys()):
            if key not in variables or shared[key] != variables[key]:
                shared.pop(key)
        if not shared:
            return None
    return shared


def _strip_metadata(node) -> None:
    if isinstance(node, (dict, OrderedDict)):
        keys = list(node.keys())
        for key in keys:
            value = node[key]
            if key == "Metadata" and isinstance(value, (dict, OrderedDict)):
                value.pop("aws:cdk:path", None)
                if not value:
                    node.pop(key)
                    continue
            _strip_metadata(value)
    elif isinstance(node, list):
        for item in node:
            _strip_metadata(item)
