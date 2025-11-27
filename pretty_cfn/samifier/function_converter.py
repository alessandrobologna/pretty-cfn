"""Lambda and API conversion utilities for SAM refactors."""

from __future__ import annotations

from collections import OrderedDict
from pathlib import Path
from typing import Iterable, Optional, Tuple

from ..formatter import CFNTag
from .asset_stager import SamAssetStager
from .event_sources import (
    fold_cognito_triggers,
    fold_event_source_mappings,
    fold_iot_rules,
    fold_push_events,
)
from .api_gateway import fold_api_shells, fold_apigateway_methods, fold_http_api_shells
from .lambda_converter import convert_lambda_function, merge_role_policies, maybe_remove_basic_role
from .state_machines import convert_state_machines
from .optimizations import convert_simple_tables, convert_layers
from .shared import _ensure_sam_transform, _extract_logical_id, _format_code_uri, _sanitize_path


def samify_template(
    template: dict,
    *,
    asset_search_paths: Iterable[Path],
    relative_to: Optional[Path] = None,
    asset_stager: Optional[SamAssetStager] = None,
    prefer_external_assets: bool = False,
) -> Tuple[dict, bool]:
    """Convert supported AWS::Lambda::Function resources into SAM functions."""

    resources = template.get("Resources")
    if not isinstance(resources, (dict, OrderedDict)):
        return template, False

    search_roots = [_sanitize_path(p) for p in asset_search_paths if p is not None]
    changed = False

    converted_functions, lambda_changed = _convert_lambda_functions(
        template,
        resources=resources,
        search_roots=search_roots,
        relative_to=relative_to,
        asset_stager=asset_stager,
        prefer_external_assets=prefer_external_assets,
    )
    changed = changed or lambda_changed

    if converted_functions:
        integrations_changed = _fold_function_integrations(template, converted_functions)
        changed = changed or integrations_changed

    template, additional_changed = _convert_additional_resources(
        template,
        converted_functions=converted_functions,
        search_roots=search_roots,
        relative_to=relative_to,
        asset_stager=asset_stager,
        prefer_external_assets=prefer_external_assets,
    )
    changed = changed or additional_changed

    if changed:
        _ensure_sam_transform(template)

    return template, changed


def _convert_lambda_functions(
    template: dict,
    *,
    resources: dict,
    search_roots: Iterable[Path],
    relative_to: Optional[Path],
    asset_stager: Optional[SamAssetStager],
    prefer_external_assets: bool,
) -> Tuple[dict[str, dict], bool]:
    converted_functions: dict[str, dict] = {}
    changed = False

    for logical_id, resource in list(resources.items()):
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::Lambda::Function":
            continue
        new_resource = convert_lambda_function(
            logical_id,
            resource,
            search_roots,
            relative_to,
            asset_stager=asset_stager,
            prefer_external_assets=prefer_external_assets,
        )
        if new_resource is None:
            continue
        resources[logical_id] = new_resource
        merge_role_policies(template, new_resource, resources)
        maybe_remove_basic_role(template, new_resource, resources)
        changed = True
        converted_functions[logical_id] = new_resource

    return converted_functions, changed


def _fold_function_integrations(
    template: dict,
    converted_functions: dict[str, dict],
) -> bool:
    if not converted_functions:
        return False

    url_changed = _fold_function_urls(template, converted_functions)
    api_changed = fold_apigateway_methods(template, converted_functions)
    esm_changed = fold_event_source_mappings(template, converted_functions)
    push_changed = fold_push_events(template, converted_functions)
    return url_changed or api_changed or esm_changed or push_changed


def _convert_additional_resources(
    template: dict,
    *,
    converted_functions: dict[str, dict],
    search_roots: Iterable[Path],
    relative_to: Optional[Path],
    asset_stager: Optional[SamAssetStager],
    prefer_external_assets: bool,
) -> Tuple[dict, bool]:
    api_shell_changed = fold_api_shells(template)
    http_api_shell_changed = fold_http_api_shells(template)

    # Always attempt higher-level conversions once base SAM work is done
    sm_changed = convert_state_machines(template)
    app_sync_changed = False
    # Late import to avoid circular dependency (appsync imports this module)
    from .appsync import convert_appsync_apis  # type: ignore

    template, app_sync_changed = convert_appsync_apis(
        template,
        asset_search_paths=search_roots,
        relative_to=relative_to,
        asset_stager=asset_stager,
        prefer_external_assets=prefer_external_assets,
    )

    table_changed = convert_simple_tables(template) or False
    layer_changed = convert_layers(
        template,
        asset_search_paths=search_roots,
        relative_to=relative_to,
        asset_stager=asset_stager,
    )

    iot_changed = fold_iot_rules(template, converted_functions)
    cognito_changed = fold_cognito_triggers(template, converted_functions)

    changed = any(
        (
            api_shell_changed,
            http_api_shell_changed,
            sm_changed,
            app_sync_changed,
            table_changed,
            layer_changed,
            iot_changed,
            cognito_changed,
        )
    )
    return template, changed


def rewrite_function_url_refs(
    template: dict,
    rename_map: dict[str, str],
    *,
    rewrite_literals: bool = True,
    literal_key_allowlist: Optional[Iterable[str]] = None,
) -> None:
    if not rename_map:
        return
    allowed_keys = set(literal_key_allowlist or [])
    allowed_keys.update({"DependsOn", "Ref", "Fn::GetAtt"})
    _rewrite_function_url_refs(
        template,
        rename_map,
        rewrite_literals=rewrite_literals,
        parent_key=None,
        literal_key_allowlist=allowed_keys,
    )


def _fold_function_urls(template, converted_functions) -> bool:
    resources = template.get("Resources")
    if not isinstance(resources, (dict, OrderedDict)):
        return False

    urls_to_remove = []
    permissions_to_remove = []
    functions_with_url_config = set()
    rename_map = {}
    changed = False

    for logical_id, resource in list(resources.items()):
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::Lambda::Url":
            continue
        properties = resource.get("Properties")
        if not isinstance(properties, (dict, OrderedDict)):
            continue
        target_ref = properties.get("TargetFunctionArn")
        function_id = _extract_logical_id(target_ref)
        if not function_id:
            continue
        function_resource = converted_functions.get(function_id)
        if not isinstance(function_resource, (dict, OrderedDict)):
            continue
        function_properties = function_resource.setdefault("Properties", OrderedDict())
        if "FunctionUrlConfig" not in function_properties:
            cfg = OrderedDict()
            for key in ("AuthType", "Cors", "InvokeMode"):
                if key in properties:
                    cfg[key] = properties[key]
            if not cfg:
                continue
            function_properties["FunctionUrlConfig"] = cfg
        functions_with_url_config.add(function_id)
        urls_to_remove.append(logical_id)
        rename_map[logical_id] = f"{function_id}Url"
        changed = True

    for logical_id in urls_to_remove:
        resources.pop(logical_id, None)

    if not functions_with_url_config:
        if rename_map:
            rewrite_function_url_refs(template, rename_map)
        return changed

    for logical_id, resource in list(resources.items()):
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::Lambda::Permission":
            continue
        properties = resource.get("Properties")
        if not isinstance(properties, (dict, OrderedDict)):
            continue
        fn_ref = properties.get("FunctionName")
        function_id = _extract_logical_id(fn_ref)
        if function_id not in functions_with_url_config:
            continue
        if "FunctionUrlAuthType" not in properties and not properties.get("InvokedViaFunctionUrl"):
            continue
        permissions_to_remove.append(logical_id)

    for logical_id in permissions_to_remove:
        resources.pop(logical_id, None)
        changed = True

    if rename_map:
        rewrite_function_url_refs(template, rename_map)

    return changed


def _convert_events_rule(rule_props: OrderedDict, target: OrderedDict) -> Optional[OrderedDict]:
    # If EventPattern present, map to EventBridgeRule; otherwise Schedule
    event: OrderedDict
    if "EventPattern" in rule_props:
        # Only allow safe keys
        allowed = {"Name", "Description", "EventBusName", "EventPattern", "State"}
        if any(key not in allowed for key in rule_props.keys() if key not in {"Targets"}):
            return None
        ev_props = OrderedDict()
        ev_props["Pattern"] = rule_props.get("EventPattern")
        if "EventBusName" in rule_props:
            ev_props["EventBusName"] = rule_props["EventBusName"]
        if "State" in rule_props:
            ev_props["Enabled"] = rule_props.get("State") == "ENABLED"
        if "Description" in rule_props:
            ev_props["Description"] = rule_props["Description"]
        event = OrderedDict([("Type", "EventBridgeRule"), ("Properties", ev_props)])
    else:
        if "ScheduleExpression" not in rule_props:
            return None
        allowed = {
            "Name",
            "Description",
            "ScheduleExpression",
            "State",
            "Targets",
        }
        if any(key not in allowed for key in rule_props.keys()):
            return None
        ev_props = OrderedDict()
        ev_props["Schedule"] = rule_props.get("ScheduleExpression")
        if "State" in rule_props:
            ev_props["Enabled"] = rule_props.get("State") == "ENABLED"
        if "Description" in rule_props:
            ev_props["Description"] = rule_props["Description"]
        if "Input" in target:
            ev_props["Input"] = target["Input"]
        if "DeadLetterConfig" in target:
            ev_props["DeadLetterConfig"] = target["DeadLetterConfig"]
        if "RetryPolicy" in target:
            ev_props["RetryPolicy"] = target["RetryPolicy"]
        event = OrderedDict([("Type", "Schedule"), ("Properties", ev_props)])
    return event


def _find_lambda_permissions_for_rule(resources: dict, rule_id: str, function_id: str) -> list[str]:
    perm_ids: list[str] = []
    for logical_id, resource in list(resources.items()):
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::Lambda::Permission":
            continue
        props = resource.get("Properties")
        if not isinstance(props, (dict, OrderedDict)):
            continue
        if _extract_logical_id(props.get("FunctionName")) != function_id:
            continue
        source = props.get("SourceArn")
        if not source:
            continue
        if rule_id in repr(source):
            perm_ids.append(logical_id)
    return perm_ids


def _rewrite_function_url_refs(
    node,
    rename_map: dict,
    *,
    rewrite_literals: bool,
    parent_key: Optional[str],
    literal_key_allowlist: set[str],
) -> None:
    if isinstance(node, (str, int, float, bool)) or node is None:
        return
    if isinstance(node, CFNTag):
        _rewrite_cfn_tag(node, rename_map, rewrite_literals, literal_key_allowlist)
        return
    if isinstance(node, list):
        for idx, item in enumerate(node):
            if isinstance(item, str):
                should_rewrite = rewrite_literals or (
                    parent_key in literal_key_allowlist if parent_key else False
                )
                if should_rewrite:
                    replacement = _rewrite_string_reference(item, rename_map)
                    if replacement is not None:
                        node[idx] = replacement
                        continue
            _rewrite_function_url_refs(
                item,
                rename_map,
                rewrite_literals=rewrite_literals,
                parent_key=parent_key,
                literal_key_allowlist=literal_key_allowlist,
            )
        return
    if isinstance(node, (dict, OrderedDict)):
        for key, value in list(node.items()):
            if isinstance(value, str):
                should_rewrite = rewrite_literals or key in literal_key_allowlist
                if should_rewrite:
                    replacement = _rewrite_string_reference(value, rename_map)
                    if replacement is not None:
                        node[key] = replacement
                        continue
            _rewrite_function_url_refs(
                value,
                rename_map,
                rewrite_literals=rewrite_literals,
                parent_key=key,
                literal_key_allowlist=literal_key_allowlist,
            )


def _rewrite_cfn_tag(
    tag_obj: CFNTag, rename_map: dict, rewrite_literals: bool, literal_key_allowlist: set[str]
) -> None:
    if tag_obj.tag == "GetAtt":
        target = tag_obj.value
        if isinstance(target, list) and target:
            logical_id = target[0]
            if logical_id in rename_map:
                new_target = list(target)
                new_target[0] = rename_map[logical_id]
                tag_obj.value = new_target
        elif isinstance(target, str) and target:
            base, sep, rest = target.partition(".")
            if base in rename_map:
                tag_obj.value = f"{rename_map[base]}{sep}{rest}" if sep else rename_map[base]
        else:
            _rewrite_function_url_refs(
                target,
                rename_map,
                rewrite_literals=rewrite_literals,
                parent_key=None,
                literal_key_allowlist=literal_key_allowlist,
            )
        return
    if tag_obj.tag == "Ref":
        ref = tag_obj.value
        if isinstance(ref, str) and ref in rename_map:
            tag_obj.value = rename_map[ref]
        return

    inner = tag_obj.value
    if isinstance(inner, str):
        if rewrite_literals:
            replacement = _rewrite_string_reference(inner, rename_map)
            if replacement is not None:
                tag_obj.value = replacement
    else:
        _rewrite_function_url_refs(
            inner,
            rename_map,
            rewrite_literals=rewrite_literals,
            parent_key=None,
            literal_key_allowlist=literal_key_allowlist,
        )


def _rewrite_string_reference(value: str, rename_map: dict) -> Optional[str]:
    if value in rename_map:
        return rename_map[value]
    if "." in value:
        base, rest = value.split(".", 1)
        if base in rename_map:
            return f"{rename_map[base]}.{rest}"
    return None


def _rewrite_code_uri_paths(template: dict, stager: SamAssetStager, relative_base: Path) -> None:
    resources = template.get("Resources")
    if not isinstance(resources, (dict, OrderedDict)):
        return
    for record in stager.records:
        resource = resources.get(record.logical_id)
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        properties = resource.setdefault("Properties", OrderedDict())
        properties["CodeUri"] = _format_code_uri(record.staged_path, relative_base)
