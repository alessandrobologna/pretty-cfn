"""Shared helpers for SAM refactor conversions."""

from __future__ import annotations

import re
from collections import OrderedDict
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, MutableMapping, Optional

from ..formatter import CFNTag, LiteralStr

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .asset_stager import AwsEnvironment


def _sanitize_path(path_obj: Path) -> Path:
    try:
        return path_obj.resolve()
    except OSError:
        return path_obj


def _format_code_uri(path_obj: Path, relative_to: Optional[Path]) -> str:
    if relative_to is not None:
        try:
            return path_obj.resolve().relative_to(relative_to.resolve()).as_posix()
        except (ValueError, OSError):
            pass
    try:
        return path_obj.resolve().as_posix()
    except OSError:
        return path_obj.as_posix()


def _ensure_sam_transform(template: MutableMapping[str, Any]) -> None:
    transform_entry = "AWS::Serverless-2016-10-31"
    existing = template.get("Transform")
    if existing is None:
        template["Transform"] = transform_entry
    elif isinstance(existing, list):
        if transform_entry not in existing:
            existing.append(transform_entry)
    elif isinstance(existing, str):
        if existing != transform_entry:
            template["Transform"] = [existing, transform_entry]


def _remove_resources(template, logical_ids):
    if not logical_ids:
        return
    resources = template.get("Resources")
    if not isinstance(resources, (dict, OrderedDict)):
        return
    targets = set(logical_ids)
    for logical_id in logical_ids:
        resources.pop(logical_id, None)
    for resource in resources.values():
        depends_on = resource.get("DependsOn")
        if isinstance(depends_on, list):
            new_list = [
                entry for entry in depends_on if not (isinstance(entry, str) and entry in targets)
            ]
            if new_list:
                resource["DependsOn"] = new_list
            elif "DependsOn" in resource:
                resource.pop("DependsOn")
        elif isinstance(depends_on, str):
            if depends_on in targets:
                resource.pop("DependsOn", None)


def _extract_logical_id(value) -> Optional[str]:
    if isinstance(value, str):
        if "." in value:
            return value.split(".", 1)[0]
        return value
    if isinstance(value, CFNTag):
        if value.tag == "GetAtt":
            target = value.value
            if isinstance(target, list) and target:
                return target[0]
            if isinstance(target, str) and target:
                return target.split(".", 1)[0]
        if value.tag == "Ref":
            ref = value.value
            if isinstance(ref, str):
                return ref
    if isinstance(value, (dict, OrderedDict)):
        if "Fn::GetAtt" in value:
            target = value["Fn::GetAtt"]
            if isinstance(target, list) and target:
                return target[0]
            if isinstance(target, str) and target:
                return target.split(".", 1)[0]
        if "Ref" in value:
            ref = value["Ref"]
            if isinstance(ref, str):
                return ref
    return None


def _build_api_resource_paths(resources: dict) -> dict[str, str]:
    cache: dict[str, str] = {}

    def resolve(logical_id: str) -> Optional[str]:
        if logical_id in cache:
            return cache[logical_id]
        resource = resources.get(logical_id)
        if not isinstance(resource, (dict, OrderedDict)):
            return None
        if resource.get("Type") != "AWS::ApiGateway::Resource":
            return None
        props = resource.get("Properties") or {}
        parent = props.get("ParentId")
        parent_path = _parent_path(parent)
        if parent_path is None:
            parent_id = _extract_logical_id(parent)
            if parent_id is None:
                return None
            parent_path = resolve(parent_id)
        if parent_path is None:
            return None
        path_part = props.get("PathPart")
        if not isinstance(path_part, str):
            return None
        new_path = _join_paths(parent_path, path_part)
        cache[logical_id] = new_path
        return new_path

    def _parent_path(value) -> Optional[str]:
        if isinstance(value, CFNTag) and value.tag == "GetAtt":
            target = value.value
            if isinstance(target, list) and len(target) >= 2 and target[1] == "RootResourceId":
                return "/"
            if isinstance(target, str) and target.endswith(".RootResourceId"):
                return "/"
        if isinstance(value, (dict, OrderedDict)) and "Fn::GetAtt" in value:
            target = value["Fn::GetAtt"]
            if isinstance(target, list) and len(target) >= 2 and target[1] == "RootResourceId":
                return "/"
            if isinstance(target, str) and target.endswith(".RootResourceId"):
                return "/"
        return None

    for logical_id in list(resources.keys()):
        resolve(logical_id)
    return cache


def _join_paths(parent: Optional[str], child: str) -> str:
    parent = parent or "/"
    if parent == "/":
        if child:
            return f"/{child}"
        return "/"
    if not child:
        return parent
    if parent.endswith("/"):
        parent = parent.rstrip("/")
    return f"{parent}/{child}"


def _is_lambda_proxy_integration(integration) -> bool:
    if not isinstance(integration, (dict, OrderedDict)):
        return False
    integration_type = integration.get("Type")
    if isinstance(integration_type, str) and integration_type.upper() != "AWS_PROXY":
        return False
    return _function_from_integration(integration) is not None


def _function_from_integration(integration) -> Optional[str]:
    if isinstance(integration, (dict, OrderedDict)):
        uri = integration.get("Uri") or integration.get("IntegrationUri")
    else:
        uri = None
    if isinstance(uri, CFNTag):
        if uri.tag == "GetAtt":
            target = uri.value
            if isinstance(target, list) and target:
                return target[0]
            if isinstance(target, str):
                return target.split(".", 1)[0]
        if uri.tag == "Sub":
            template = uri.value[0] if isinstance(uri.value, list) else uri.value
            if isinstance(template, str):
                matches = re.findall(r"\${([A-Za-z0-9]+)\.Arn}", template)
                if matches:
                    return matches[0]
        if uri.tag == "Join":
            parts = uri.value[1] if isinstance(uri.value, list) and len(uri.value) == 2 else []
            for part in parts:
                if isinstance(part, CFNTag) and part.tag == "GetAtt":
                    target = part.value
                    if isinstance(target, list) and target:
                        return target[0]
                    if isinstance(target, str):
                        return target.split(".", 1)[0]
    if isinstance(uri, (dict, OrderedDict)):
        if "Fn::GetAtt" in uri:
            target = uri["Fn::GetAtt"]
            if isinstance(target, list) and target:
                return target[0]
            if isinstance(target, str):
                return target.split(".", 1)[0]
        if "Fn::Sub" in uri:
            template = uri["Fn::Sub"]
            if isinstance(template, list) and template:
                template = template[0]
            if isinstance(template, str):
                matches = re.findall(r"\${([A-Za-z0-9]+)\.Arn}", template)
                if matches:
                    return matches[0]
        if "Fn::Join" in uri:
            parts = uri["Fn::Join"]
            items = parts[1] if isinstance(parts, list) and len(parts) == 2 else []
            for part in items:
                if isinstance(part, (dict, OrderedDict)) and "Fn::GetAtt" in part:
                    target = part["Fn::GetAtt"]
                    if isinstance(target, list) and target:
                        return target[0]
                    if isinstance(target, str):
                        return target.split(".", 1)[0]
    if isinstance(uri, str):
        match = re.search(r"functions/(?P<name>[A-Za-z0-9]+)\/invocations", uri)
        if match:
            return match.group("name")
    return None


def _resolve_method_path(resource_id, path_cache: dict[str, str]) -> Optional[str]:
    if isinstance(resource_id, CFNTag) and resource_id.tag == "GetAtt":
        target = resource_id.value
        if isinstance(target, list) and len(target) >= 2 and target[1] == "RootResourceId":
            return "/"
        if isinstance(target, str) and target.endswith(".RootResourceId"):
            return "/"
    if isinstance(resource_id, (dict, OrderedDict)) and "Fn::GetAtt" in resource_id:
        target = resource_id["Fn::GetAtt"]
        if isinstance(target, list) and len(target) >= 2 and target[1] == "RootResourceId":
            return "/"
        if isinstance(target, str) and target.endswith(".RootResourceId"):
            return "/"
    target_id = _extract_logical_id(resource_id)
    if target_id is None:
        return None
    return path_cache.get(target_id)


def _attach_api_event(function_resource, rest_api_id, method, path) -> bool:
    if not isinstance(function_resource, (dict, OrderedDict)):
        return False
    props = function_resource.setdefault("Properties", OrderedDict())
    events = props.setdefault("Events", OrderedDict())
    method = (method or "ANY").upper()
    event_name = _generate_event_name(events, method, path)
    event_entry = OrderedDict()
    event_entry["Type"] = "Api"
    event_props = OrderedDict()
    if rest_api_id is not None:
        event_props["RestApiId"] = rest_api_id
    event_props["Path"] = path
    event_props["Method"] = method
    event_entry["Properties"] = event_props
    events[event_name] = event_entry
    return True


def _generate_event_name(events: OrderedDict, method: str, path: str) -> str:
    base = f"Api{method.title()}{_sanitize_path_for_name(path)}"
    if base not in events:
        return base
    idx = 2
    while f"{base}{idx}" in events:
        idx += 1
    return f"{base}{idx}"


def _sanitize_path_for_name(path: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", " ", path or "")
    parts = [part.capitalize() for part in cleaned.split() if part]
    return "".join(parts) or "Root"


def _find_apigw_permissions(resources, function_id: str, rest_api_id) -> list[str]:
    logical_ids = []
    rest_api_name = _extract_logical_id(rest_api_id)
    for logical_id, resource in list(resources.items()):
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::Lambda::Permission":
            continue
        props = resource.get("Properties")
        if not isinstance(props, (dict, OrderedDict)):
            continue
        if props.get("Principal") != "apigateway.amazonaws.com":
            continue
        fn_ref = _extract_logical_id(props.get("FunctionName"))
        if fn_ref != function_id:
            continue
        source_arn = props.get("SourceArn")
        if rest_api_name and not _source_arn_refers_to_api(source_arn, rest_api_name):
            continue
        metadata = resource.get("Metadata") or {}
        if "ApiGateway" not in metadata.get("aws:cdk:path", ""):
            continue
        logical_ids.append(logical_id)
    return logical_ids


def _source_arn_refers_to_api(source_arn, rest_api_id: str) -> bool:
    if isinstance(source_arn, CFNTag) and source_arn.tag == "Sub":
        template = source_arn.value[0] if isinstance(source_arn.value, list) else source_arn.value
        if isinstance(template, str):
            return f"${{{rest_api_id}}}" in template or rest_api_id in template
    if isinstance(source_arn, (dict, OrderedDict)) and "Fn::Sub" in source_arn:
        template = source_arn["Fn::Sub"]
        if isinstance(template, list) and template:
            template = template[0]
        if isinstance(template, str):
            return f"${{{rest_api_id}}}" in template or rest_api_id in template
    if isinstance(source_arn, CFNTag) and source_arn.tag == "Join":
        parts = (
            source_arn.value[1]
            if isinstance(source_arn.value, list) and len(source_arn.value) == 2
            else []
        )
        for part in parts:
            if isinstance(part, CFNTag) and part.tag == "Ref" and part.value == rest_api_id:
                return True
            if isinstance(part, str) and rest_api_id in part:
                return True
        return False
    if isinstance(source_arn, (dict, OrderedDict)) and "Fn::Join" in source_arn:
        join_parts = source_arn["Fn::Join"]
        items = join_parts[1] if isinstance(join_parts, list) and len(join_parts) == 2 else []
        for part in items:
            if isinstance(part, (dict, OrderedDict)) and part.get("Ref") == rest_api_id:
                return True
            if isinstance(part, str) and rest_api_id in part:
                return True
        return False
    if isinstance(source_arn, str):
        return rest_api_id in source_arn
    return False


def _ensure_sub_tag(value):
    if isinstance(value, CFNTag):
        return value
    if isinstance(value, dict) and "Fn::Sub" in value:
        return CFNTag("Sub", value["Fn::Sub"])
    return value


def _prepare_inline_code(value: str) -> str:
    decoded = _decode_escaped_string(value)
    lines = decoded.split("\n")
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    if not lines:
        return ""
    indent = None
    for line in lines:
        if not line.strip():
            continue
        leading = len(line) - len(line.lstrip(" "))
        if indent is None or leading < indent:
            indent = leading
    if indent:
        lines = [line[indent:] if len(line) >= indent else line for line in lines]
    # Tabs confuse YAML literal emitters; expand them so SchemaInline stays a block scalar
    lines = [line.expandtabs(2) for line in lines]
    return LiteralStr("\n".join(lines))


def _decode_escaped_string(value: str) -> str:
    try:
        decoded = bytes(value, "utf-8").decode("unicode_escape")
    except Exception:
        decoded = value.replace("\\n", "\n")
    return decoded.replace("\r\n", "\n")


def _extract_sub_parts(value) -> tuple[Optional[str], Optional[dict[str, object]]]:
    if isinstance(value, str):
        return value, None
    if isinstance(value, list) and len(value) == 2:
        template = value[0] if isinstance(value[0], str) else None
        mapping = value[1] if isinstance(value[1], dict) else None
        return template, mapping
    return None, None


def _download_s3_object(bucket: str, key: str, version: Optional[str], target: Path) -> None:
    try:
        import boto3
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("boto3 is required to download Lambda assets from S3") from exc

    session = boto3.session.Session()
    client = session.client("s3")
    extra_args: dict[str, str] = {}
    if version:
        extra_args["VersionId"] = version

    with target.open("wb") as handle:
        client.download_fileobj(bucket, key, handle, ExtraArgs=extra_args or None)


def _format_s3_uri(bucket: str, key: str, version: Optional[str]) -> str:
    base = f"s3://{bucket}/{key}"
    if version:
        return f"{base}?versionId={version}"
    return base


def _detect_aws_env() -> Optional["AwsEnvironment"]:
    try:
        import boto3
    except ImportError:  # pragma: no cover - optional dependency
        return None

    from .asset_stager import AwsEnvironment  # lazy import to avoid cycles

    session = boto3.session.Session()
    region = session.region_name or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")

    try:
        sts = session.client("sts")
        identity = sts.get_caller_identity()
        account_id = identity.get("Account")
    except Exception:  # pragma: no cover - AWS environment issues
        return None

    if not account_id:
        return None

    if not region:
        region = "us-east-1"

    partition = _infer_partition(region)
    return AwsEnvironment(account_id=account_id, region=region, partition=partition)


def _infer_partition(region: Optional[str]) -> str:
    if not region:
        return "aws"
    lowered = region.lower()
    if lowered.startswith("us-gov"):
        return "aws-us-gov"
    if lowered.startswith("cn-"):
        return "aws-cn"
    return "aws"
