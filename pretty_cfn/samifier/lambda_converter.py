"""Helpers for converting AWS::Lambda::Function resources into SAM functions."""

from __future__ import annotations

from collections import OrderedDict
from pathlib import Path
from typing import Iterable, MutableMapping, Optional, Any

import copy
import re

from ..formatter import CFNTag
from .asset_stager import SamAssetStager
from .shared import (
    _ensure_sub_tag,
    _extract_logical_id,
    _format_code_uri,
    _prepare_inline_code,
    _remove_resources,
)


def convert_lambda_function(
    logical_id: str,
    resource: MutableMapping[str, Any],
    search_roots: Iterable[Path],
    relative_to: Optional[Path],
    *,
    asset_stager: Optional[SamAssetStager] = None,
    prefer_external_assets: bool = False,
) -> Optional[MutableMapping[str, Any]]:
    """Convert a single AWS::Lambda::Function into AWS::Serverless::Function."""

    properties = resource.get("Properties")
    if not isinstance(properties, (dict, OrderedDict)):
        return None
    properties = OrderedDict(properties)

    metadata = resource.get("Metadata") or {}
    asset_path = metadata.get("aws:asset:path")
    asset_property = metadata.get("aws:asset:property")
    code = properties.get("Code")

    new_properties = OrderedDict()
    code_handled = False
    missing_local_asset: Optional[Path] = None

    if asset_path and (not asset_property or asset_property == "Code"):
        resolved_path = _resolve_asset_uri(asset_path, search_roots)
        if resolved_path is None:
            return None
        if resolved_path.exists():
            if asset_stager:
                staged_path = asset_stager.stage_local_path(logical_id, resolved_path)
                code_uri = _format_code_uri(staged_path, relative_to)
            else:
                code_uri = _format_code_uri(resolved_path, relative_to)
            new_properties["CodeUri"] = code_uri
            code_handled = True
        else:
            missing_local_asset = resolved_path
    if not code_handled and isinstance(code, (dict, OrderedDict)):
        code = OrderedDict(code)
        if "ZipFile" in code:
            inline_literal = _prepare_inline_code(code["ZipFile"])
            if prefer_external_assets and asset_stager is not None:
                file_name = _infer_inline_handler_filename(properties)
                staged_file = asset_stager.stage_inline_text(
                    logical_id,
                    str(inline_literal),
                    file_name=file_name,
                )
                new_properties["CodeUri"] = _format_code_uri(staged_file.parent, relative_to)
            else:
                new_properties["InlineCode"] = inline_literal
            code_handled = True
        elif _looks_like_s3_code(code):
            bucket_value = code.get("S3Bucket")
            key_value = code.get("S3Key")
            version = code.get("S3ObjectVersion")
            resolved_bucket = None
            resolved_key = None
            if asset_stager:
                resolved_bucket = asset_stager.resolve_string(_ensure_sub_tag(bucket_value))
                resolved_key = asset_stager.resolve_string(_ensure_sub_tag(key_value))
            if resolved_bucket is None and isinstance(bucket_value, str):
                resolved_bucket = bucket_value
            if resolved_key is None and isinstance(key_value, str):
                resolved_key = key_value

            if asset_stager and resolved_bucket and resolved_key:
                staged_dir = asset_stager.stage_s3_code(
                    logical_id, resolved_bucket, resolved_key, version
                )
                new_properties["CodeUri"] = _format_code_uri(staged_dir, relative_to)
            else:
                function_code = OrderedDict()
                function_code["Bucket"] = bucket_value
                function_code["Key"] = key_value
                if version:
                    function_code["Version"] = version
                new_properties["CodeUri"] = function_code
            code_handled = True

    if not code_handled and missing_local_asset is not None:
        new_properties["CodeUri"] = _format_code_uri(missing_local_asset, relative_to)
        code_handled = True

    if not code_handled:
        return None

    for key, value in properties.items():
        if key == "Code":
            continue
        new_properties[key] = value

    # Mutate the existing resource mapping in place so that any attached
    # comments (on the resource logical ID or its Type) are preserved.
    resource["Type"] = "AWS::Serverless::Function"
    resource["Properties"] = new_properties
    return resource


def _resolve_asset_uri(asset_path: str, search_roots: Iterable[Path]) -> Optional[Path]:
    candidate = Path(asset_path)
    candidates = []
    if candidate.is_absolute():
        candidates.append(candidate)
    else:
        for root in search_roots:
            candidates.append(root / candidate)
        candidates.append(candidate)

    resolved: Optional[Path] = None
    for entry in candidates:
        try:
            if entry.exists():
                resolved = entry.resolve()
                break
        except OSError:
            continue

    if resolved is None and candidates:
        try:
            resolved = candidates[0].resolve()
        except OSError:
            resolved = candidates[0]

    return resolved


def _looks_like_s3_code(code: OrderedDict) -> bool:
    return "S3Bucket" in code and "S3Key" in code


def _infer_inline_handler_filename(properties: OrderedDict) -> str:
    handler = properties.get("Handler")
    runtime = properties.get("Runtime")
    base = "index"
    if isinstance(handler, str) and handler.strip():
        base = handler.split("::", 1)[0]
        base = base.split(".", 1)[0]
        base = base.split("/", 1)[-1]
        # Keep filename portable and stable
        base = re.sub(r"[^A-Za-z0-9_\\-]", "_", base) or "index"
    extension = _runtime_extension(runtime)
    return f"{base}{extension}"


def _runtime_extension(runtime) -> str:
    if not isinstance(runtime, str):
        return ".js"
    lowered = runtime.lower()
    if lowered.startswith("python"):
        return ".py"
    if lowered.startswith("nodejs"):
        return ".js"
    if lowered.startswith("ruby"):
        return ".rb"
    if lowered.startswith("dotnet"):
        return ".cs"
    if lowered.startswith("go"):
        return ".go"
    if lowered.startswith("java"):
        return ".java"
    if "provided" in lowered:
        return ".txt"
    return ".js"


def maybe_remove_basic_role(template: dict, function_resource: OrderedDict, resources) -> None:
    if not isinstance(function_resource, (dict, OrderedDict)):
        return

    properties = function_resource.get("Properties")
    if not isinstance(properties, (dict, OrderedDict)):
        return

    role_reference = properties.get("Role")
    if role_reference is None:
        return

    role_logical_id = _extract_logical_id(role_reference)
    if not role_logical_id:
        return

    role_resource = resources.get(role_logical_id)
    if not _is_basic_lambda_role(role_resource):
        return

    ignored_ids = {id(role_reference)}
    depends_entries = _collect_depends_entries(function_resource.get("DependsOn"), role_logical_id)
    ignored_ids.update(id(entry) for entry in depends_entries)

    if _logical_id_referenced_elsewhere(template, role_logical_id, ignored_ids):
        return

    properties.pop("Role", None)
    _prune_depends_on(function_resource, role_logical_id)
    resources.pop(role_logical_id, None)


def _is_basic_lambda_role(resource) -> bool:
    if not isinstance(resource, (dict, OrderedDict)):
        return False
    if resource.get("Type") != "AWS::IAM::Role":
        return False
    props = resource.get("Properties")
    if not isinstance(props, (dict, OrderedDict)):
        return False

    allowed_keys = {"AssumeRolePolicyDocument", "ManagedPolicyArns"}
    if set(props.keys()) - allowed_keys:
        return False

    if not _assume_role_allows_lambda(props.get("AssumeRolePolicyDocument")):
        return False

    managed = props.get("ManagedPolicyArns")
    if not isinstance(managed, list) or len(managed) != 1:
        return False
    if "AWSLambdaBasicExecutionRole" not in repr(managed[0]):
        return False

    return True


def _assume_role_allows_lambda(doc) -> bool:
    if not isinstance(doc, (dict, OrderedDict)):
        return False
    statements = doc.get("Statement")
    if not statements:
        return False
    if isinstance(statements, dict):
        statements = [statements]
    for stmt in statements:
        if not isinstance(stmt, (dict, OrderedDict)):
            continue
        effect = stmt.get("Effect")
        if effect != "Allow":
            continue
        action = stmt.get("Action")
        if isinstance(action, list):
            if "sts:AssumeRole" not in action:
                continue
        elif action != "sts:AssumeRole":
            continue
        principal = stmt.get("Principal") or {}
        service = principal.get("Service")
        services = service if isinstance(service, list) else [service]
        if "lambda.amazonaws.com" in services:
            return True
    return False


def _collect_depends_entries(depends_on, logical_id):
    entries = []
    if isinstance(depends_on, list):
        for entry in depends_on:
            if isinstance(entry, str) and entry == logical_id:
                entries.append(entry)
    elif isinstance(depends_on, str):
        if depends_on == logical_id:
            entries.append(depends_on)
    return entries


def _prune_depends_on(resource, logical_id):
    depends_on = resource.get("DependsOn")
    if isinstance(depends_on, list):
        new_list = [
            entry for entry in depends_on if not (isinstance(entry, str) and entry == logical_id)
        ]
        if new_list:
            resource["DependsOn"] = new_list
        elif "DependsOn" in resource:
            resource.pop("DependsOn")
    elif isinstance(depends_on, str):
        if depends_on == logical_id:
            resource.pop("DependsOn", None)


def _logical_id_referenced_elsewhere(node, logical_id, ignored_ids) -> bool:
    if id(node) in ignored_ids:
        return False
    if isinstance(node, CFNTag):
        if node.tag == "GetAtt":
            target = node.value
            if isinstance(target, list) and target:
                if target[0] == logical_id:
                    return True
            elif isinstance(target, str) and target:
                base = target.split(".", 1)[0]
                if base == logical_id:
                    return True
        elif node.tag == "Ref":
            if node.value == logical_id:
                return True
        return _logical_id_referenced_elsewhere(node.value, logical_id, ignored_ids)
    if isinstance(node, str):
        if node == logical_id or node.startswith(f"{logical_id}."):
            return True
        return False
    if isinstance(node, list):
        for item in node:
            if _logical_id_referenced_elsewhere(item, logical_id, ignored_ids):
                return True
        return False
    if isinstance(node, (dict, OrderedDict)):
        for value in node.values():
            if _logical_id_referenced_elsewhere(value, logical_id, ignored_ids):
                return True
    return False


def merge_role_policies(template: dict, function_resource: OrderedDict, resources) -> None:
    properties = function_resource.get("Properties")
    if not isinstance(properties, (dict, OrderedDict)):
        return
    role_reference = properties.get("Role")
    if role_reference is None:
        return
    role_logical_id = _extract_logical_id(role_reference)
    if not role_logical_id:
        return

    collected = _collect_role_policies(resources, role_logical_id)
    if not collected:
        return

    policies_prop = properties.setdefault("Policies", [])
    removal_ids: list[str] = []
    for logical_id, policy_doc in collected:
        policies_prop.extend(_convert_policy_document(policy_doc))
        _prune_depends_on(function_resource, logical_id)
        removal_ids.append(logical_id)

    if removal_ids:
        _remove_resources(template, removal_ids)


def _collect_role_policies(resources, role_logical_id: str) -> list[tuple[str, OrderedDict]]:
    collected: list[tuple[str, OrderedDict]] = []
    for logical_id, resource in list(resources.items()):
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::IAM::Policy":
            continue
        properties = resource.get("Properties")
        if not isinstance(properties, (dict, OrderedDict)):
            continue
        roles = properties.get("Roles")
        if not isinstance(roles, list) or not roles:
            continue
        if not any(_extract_logical_id(role) == role_logical_id for role in roles):
            continue
        policy_doc = properties.get("PolicyDocument")
        if isinstance(policy_doc, (dict, OrderedDict)):
            policy_doc = OrderedDict(policy_doc)
            collected.append((logical_id, policy_doc))
    return collected


def _convert_policy_document(policy_doc: OrderedDict) -> list[OrderedDict]:
    statements = policy_doc.get("Statement")
    if not statements:
        return [policy_doc]
    if isinstance(statements, (dict, OrderedDict)):
        statements_list = [statements]
    else:
        statements_list = list(statements)

    template_matches: list[OrderedDict] = []

    for matcher in (_match_s3_policy_template, _match_sqs_policy_template):
        matched, statements_list = matcher(statements_list)
        template_matches.extend(matched)

    dynamodb_statements = [stmt for stmt in statements_list if _is_dynamodb_statement(stmt)]
    other_statements = [stmt for stmt in statements_list if stmt not in dynamodb_statements]

    results: list[OrderedDict] = []
    results.extend(template_matches)
    table_ref = _detect_single_table_resource(dynamodb_statements)
    if table_ref is not None:
        template_entry = OrderedDict()
        template_entry["DynamoDBCrudPolicy"] = OrderedDict([("TableName", table_ref)])
        results.append(template_entry)
    else:
        other_statements.extend(dynamodb_statements)

    if other_statements:
        inline_doc = copy.deepcopy(policy_doc)
        inline_doc["Statement"] = other_statements
        results.append(inline_doc)

    if not results:
        return [policy_doc]
    return results


def _match_s3_policy_template(statements: list) -> tuple[list[OrderedDict], list]:
    if not statements:
        return [], statements
    read_actions = {
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket",
        "s3:ListBucketVersions",
    }
    crud_actions = read_actions | {
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload",
    }

    matched: list[OrderedDict] = []
    remaining: list = []

    for stmt in statements:
        actions = _actions_as_set(stmt)
        resources = _resources_as_list(stmt)
        if not actions or not resources:
            remaining.append(stmt)
            continue
        bucket = _bucket_name_from_resources(resources)
        if bucket is None:
            remaining.append(stmt)
            continue
        if actions.issubset(read_actions):
            matched.append(OrderedDict([("S3ReadPolicy", OrderedDict([("BucketName", bucket)]))]))
            continue
        if actions.issubset(crud_actions):
            matched.append(OrderedDict([("S3CrudPolicy", OrderedDict([("BucketName", bucket)]))]))
            continue
        remaining.append(stmt)

    return matched, remaining


def _match_sqs_policy_template(statements: list) -> tuple[list[OrderedDict], list]:
    poller_actions = {
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes",
        "sqs:GetQueueUrl",
        "sqs:ChangeMessageVisibility",
    }
    matched: list[OrderedDict] = []
    remaining: list = []
    matched_stmt_objs: set[int] = set()
    for stmt in statements:
        actions = _actions_as_set(stmt)
        resources = _resources_as_list(stmt)
        if not actions or not resources:
            remaining.append(stmt)
            continue
        if not actions.issubset(poller_actions):
            remaining.append(stmt)
            continue
        queue = _queue_name_from_resources(resources)
        if queue is None:
            remaining.append(stmt)
            continue
        matched.append(OrderedDict([("SQSPollerPolicy", OrderedDict([("QueueName", queue)]))]))
        matched_stmt_objs.add(id(stmt))

    remaining = [stmt for stmt in statements if id(stmt) not in matched_stmt_objs]
    return matched, remaining


def _actions_as_set(statement) -> Optional[set[str]]:
    if not isinstance(statement, (dict, OrderedDict)):
        return None
    actions = statement.get("Action")
    if actions is None:
        return None
    if isinstance(actions, str):
        return {actions}
    if isinstance(actions, list):
        return {a for a in actions if isinstance(a, str)}
    return None


def _resources_as_list(statement) -> list:
    if not isinstance(statement, (dict, OrderedDict)):
        return []
    resources = statement.get("Resource")
    if resources is None:
        return []
    if isinstance(resources, list):
        return resources
    return [resources]


def _bucket_name_from_resources(resources: list) -> Optional[object]:
    name = None
    for res in resources:
        candidate = _bucket_name_from_resource(res)
        if candidate is None:
            return None
        if name is None:
            name = candidate
        elif name != candidate:
            return None
    return name


def _bucket_name_from_resource(value) -> Optional[object]:
    if isinstance(value, (dict, OrderedDict)) and "Ref" in value and isinstance(value["Ref"], str):
        return OrderedDict([("Ref", value["Ref"])])
    if isinstance(value, CFNTag) and value.tag == "Ref" and isinstance(value.value, str):
        return OrderedDict([("Ref", value.value)])
    if isinstance(value, (dict, OrderedDict)) and "Fn::GetAtt" in value:
        target = value["Fn::GetAtt"]
        if isinstance(target, list) and len(target) >= 2 and target[1] == "Arn":
            return OrderedDict([("Ref", target[0])])
        if isinstance(target, str) and target.endswith(".Arn"):
            return OrderedDict([("Ref", target.split(".", 1)[0])])
    if isinstance(value, CFNTag) and value.tag == "GetAtt":
        target = value.value
        if isinstance(target, list) and len(target) >= 2 and target[1] == "Arn":
            return OrderedDict([("Ref", target[0])])
        if isinstance(target, str) and target.endswith(".Arn"):
            return OrderedDict([("Ref", target.split(".", 1)[0])])
    if isinstance(value, str) and value.startswith("arn:"):
        parts = value.split(":")
        if len(parts) >= 6 and parts[5]:
            bucket_part = parts[5].split("/", 1)[0]
            if bucket_part:
                return bucket_part
    if isinstance(value, (dict, OrderedDict)) and "Fn::Sub" in value:
        sub_val = value.get("Fn::Sub")
        if isinstance(sub_val, str):
            if "${" in sub_val and ".Arn" in sub_val:
                inner = sub_val.split("${", 1)[1].split("}", 1)[0]
                logical = inner.split(".Arn", 1)[0]
                if logical:
                    return OrderedDict([("Ref", logical)])
    if isinstance(value, str) and "${" in value and ".Arn" in value:
        inner = value.split("${", 1)[1].split("}", 1)[0]
        logical = inner.split(".Arn", 1)[0]
        if logical:
            return OrderedDict([("Ref", logical)])
    return None


def _queue_name_from_resources(resources: list) -> Optional[object]:
    name = None
    for res in resources:
        candidate = _queue_name_from_resource(res)
        if candidate is None:
            return None
        if name is None:
            name = candidate
        elif name != candidate:
            return None
    return name


def _queue_name_from_resource(value) -> Optional[object]:
    if isinstance(value, (dict, OrderedDict)) and "Ref" in value and isinstance(value["Ref"], str):
        return OrderedDict([("Ref", value["Ref"])])
    if isinstance(value, CFNTag) and value.tag == "Ref" and isinstance(value.value, str):
        return OrderedDict([("Ref", value.value)])
    if isinstance(value, (dict, OrderedDict)) and "Fn::GetAtt" in value:
        target = value["Fn::GetAtt"]
        if isinstance(target, list) and len(target) >= 2 and target[1] == "Arn":
            return OrderedDict([("Ref", target[0])])
        if isinstance(target, str) and target.endswith(".Arn"):
            return OrderedDict([("Ref", target.split(".", 1)[0])])
    if isinstance(value, CFNTag) and value.tag == "GetAtt":
        target = value.value
        if isinstance(target, list) and len(target) >= 2 and target[1] == "Arn":
            return OrderedDict([("Ref", target[0])])
        if isinstance(target, str) and target.endswith(".Arn"):
            return OrderedDict([("Ref", target.split(".", 1)[0])])
    if isinstance(value, str) and value.startswith("arn:"):
        parts = value.split(":")
        if len(parts) >= 6 and parts[5]:
            queue_part = parts[5].split("/", 1)[0]
            if queue_part:
                return queue_part
    return None


def _detect_single_table_resource(statements: list) -> Optional[object]:
    table_name = None
    for statement in statements:
        resources = statement.get("Resource") if isinstance(statement, dict) else None
        if resources is None:
            return None
        resource_entries = resources if isinstance(resources, list) else [resources]
        statement_tables = {
            _table_name_from_resource(entry)
            for entry in resource_entries
            if _table_name_from_resource(entry)
        }
        statement_tables.discard("AWS::NoValue")
        if not statement_tables:
            continue
        if len(statement_tables) != 1:
            return None
        table = statement_tables.pop()
        if table_name is None:
            table_name = table
        elif table_name != table:
            return None
    if table_name is None:
        return None
    return OrderedDict([("Ref", table_name)])


def _table_name_from_resource(value) -> Optional[str]:
    if isinstance(value, CFNTag) and value.tag == "GetAtt":
        target = value.value
        if isinstance(target, list) and len(target) >= 2 and target[1] == "Arn":
            return target[0]
        if isinstance(target, str) and target.endswith(".Arn"):
            return target.split(".", 1)[0]
        return None
    if isinstance(value, (dict, OrderedDict)):
        if "Fn::GetAtt" in value:
            target = value["Fn::GetAtt"]
            if isinstance(target, list) and len(target) >= 2 and target[1] == "Arn":
                return target[0]
            if isinstance(target, str) and target.endswith(".Arn"):
                return target.split(".", 1)[0]
    if value == OrderedDict([("Ref", "AWS::NoValue")]) or value == {"Ref": "AWS::NoValue"}:
        return "AWS::NoValue"
    return None


def _is_dynamodb_statement(statement) -> bool:
    if not isinstance(statement, (dict, OrderedDict)):
        return False
    actions = statement.get("Action")
    if actions is None:
        return False
    action_list = actions if isinstance(actions, list) else [actions]
    if not action_list:
        return False
    allowed = {
        "dynamodb:BatchGetItem",
        "dynamodb:GetRecords",
        "dynamodb:GetShardIterator",
        "dynamodb:Query",
        "dynamodb:GetItem",
        "dynamodb:Scan",
        "dynamodb:ConditionCheckItem",
        "dynamodb:BatchWriteItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:DeleteItem",
        "dynamodb:DescribeTable",
    }
    for action in action_list:
        if not isinstance(action, str) or action not in allowed:
            return False
    resources = statement.get("Resource")
    if resources is None:
        return False
    resource_list = resources if isinstance(resources, list) else [resources]
    has_table = any(
        _table_name_from_resource(entry) not in (None, "AWS::NoValue") for entry in resource_list
    )
    return has_table
