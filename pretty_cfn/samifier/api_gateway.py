"""Helpers for folding API Gateway and HttpApi resources into SAM constructs."""

from __future__ import annotations

from collections import OrderedDict
from typing import NamedTuple, Optional

from ..formatter import CFNTag
from .shared import (
    _attach_api_event,
    _build_api_resource_paths,
    _extract_logical_id,
    _find_apigw_permissions,
    _function_from_integration,
    _is_lambda_proxy_integration,
    _remove_resources,
    _resolve_method_path,
    _source_arn_refers_to_api,
)


def fold_apigateway_methods(template: dict, converted_functions: dict[str, OrderedDict]) -> bool:
    resources = template.get("Resources")
    if not isinstance(resources, (dict, OrderedDict)):
        return False

    path_cache = _build_api_resource_paths(resources)
    methods_to_remove: list[str] = []
    permissions_to_remove: list[str] = []
    changed = False

    for logical_id, resource in list(resources.items()):
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::ApiGateway::Method":
            continue
        properties = resource.get("Properties")
        if not isinstance(properties, (dict, OrderedDict)):
            continue
        integration = properties.get("Integration")
        if not _is_lambda_proxy_integration(integration):
            continue
        function_id = _function_from_integration(integration)
        if not function_id or function_id not in converted_functions:
            continue
        path = _resolve_method_path(properties.get("ResourceId"), path_cache)
        if path is None:
            continue
        rest_api_id = properties.get("RestApiId")
        method = properties.get("HttpMethod") or "ANY"
        function_resource = converted_functions[function_id]
        if not _attach_api_event(function_resource, rest_api_id, method, path):
            continue
        methods_to_remove.append(logical_id)
        permissions_to_remove.extend(_find_apigw_permissions(resources, function_id, rest_api_id))
        changed = True

    _remove_resources(template, methods_to_remove)
    _remove_resources(template, permissions_to_remove)
    return changed


def fold_api_shells(template: dict) -> bool:
    resources = template.get("Resources")
    if not isinstance(resources, (dict, OrderedDict)):
        return False

    changed = False
    removals: list[str] = []
    path_cache = _build_api_resource_paths(resources)

    for logical_id, resource in list(resources.items()):
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        resource = OrderedDict(resource)
        if resource.get("Type") != "AWS::ApiGateway::RestApi":
            continue

        cors_config, cors_methods, other_methods = _detect_cors_configuration(
            resources,
            logical_id,
            path_cache,
        )
        if other_methods:
            continue

        if cors_methods and cors_config is None:
            continue

        perm_ids = _collect_apigw_permissions_for_api(resources, logical_id)
        child_resource_ids = _collect_apigw_child_resources(resources, logical_id)

        excluded_ids = set(cors_methods) | set(perm_ids) | set(child_resource_ids)
        dep_view = (
            {lid: res for lid, res in resources.items() if lid not in excluded_ids}
            if excluded_ids
            else resources
        )

        deps = _collect_apigw_deps(dep_view, logical_id)
        if deps.blocking_refs:
            continue

        stage_name = _select_stage_name(dep_view, deps.stages, logical_id)

        new_res = _convert_rest_api(resource, stage_name=stage_name, cors_config=cors_config)
        if new_res is None:
            continue

        resources[logical_id] = new_res
        removals.extend(deps.deployments)
        removals.extend(deps.stages)
        removals.extend(cors_methods)
        removals.extend(child_resource_ids)
        removals.extend(perm_ids)

        if stage_name:
            _rewrite_stage_references(template, deps.stages, stage_name)

        changed = True

    _remove_resources(template, removals)
    return changed


def fold_http_api_shells(template: dict) -> bool:
    resources = template.get("Resources")
    if not isinstance(resources, (dict, OrderedDict)):
        return False

    changed = False
    removals: list[str] = []

    for logical_id, resource in list(resources.items()):
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        resource = OrderedDict(resource)
        if resource.get("Type") != "AWS::ApiGatewayV2::Api":
            continue

        route_present = any(
            isinstance(res, (dict, OrderedDict))
            and res.get("Type") == "AWS::ApiGatewayV2::Route"
            and _extract_logical_id(res.get("Properties", {}).get("ApiId")) == logical_id
            for res in resources.values()
        )
        if route_present:
            continue

        deps = _collect_httpapi_deps(resources, logical_id)
        if deps.blocking_refs:
            continue

        new_res = _convert_http_api(resource)
        if new_res is None:
            continue

        resources[logical_id] = new_res
        removals.extend(deps.integrations)
        removals.extend(deps.routes)
        removals.extend(deps.stages)
        changed = True

    _remove_resources(template, removals)
    return changed


class _HttpApiDeps(NamedTuple):
    integrations: list[str]
    routes: list[str]
    stages: list[str]
    blocking_refs: bool


def _collect_httpapi_deps(resources: dict, api_id: str) -> _HttpApiDeps:
    integrations: list[str] = []
    routes: list[str] = []
    stages: list[str] = []
    blocking = False

    for lid, res in list(resources.items()):
        if not isinstance(res, (dict, OrderedDict)):
            continue
        res = OrderedDict(res)
        rtype = res.get("Type")
        if (
            rtype == "AWS::ApiGatewayV2::Integration"
            and _extract_logical_id(res.get("Properties", {}).get("ApiId")) == api_id
        ):
            integrations.append(lid)
        if (
            rtype == "AWS::ApiGatewayV2::Route"
            and _extract_logical_id(res.get("Properties", {}).get("ApiId")) == api_id
        ):
            routes.append(lid)
        if (
            rtype == "AWS::ApiGatewayV2::Stage"
            and _extract_logical_id(res.get("Properties", {}).get("ApiId")) == api_id
        ):
            stages.append(lid)

        if rtype not in {
            "AWS::ApiGatewayV2::Api",
            "AWS::ApiGatewayV2::Integration",
            "AWS::ApiGatewayV2::Route",
            "AWS::ApiGatewayV2::Stage",
        }:
            if _references_any(res, integrations + routes + stages + [api_id]):
                blocking = True

    return _HttpApiDeps(integrations, routes, stages, blocking)


def _convert_http_api(resource: OrderedDict) -> Optional[OrderedDict]:
    props = resource.get("Properties")
    if not isinstance(props, (dict, OrderedDict)):
        return None

    new_props = OrderedDict()
    mapping = {
        "Name": "Name",
        "Description": "Description",
        "FailOnWarnings": "FailOnWarnings",
        "CorsConfiguration": "CorsConfiguration",
        "DefaultRouteSettings": "DefaultRouteSettings",
        "RouteSettings": "RouteSettings",
        "StageVariables": "StageVariables",
        "Tags": "Tags",
        "PropagateTags": "PropagateTags",
        "DisableExecuteApiEndpoint": "DisableExecuteApiEndpoint",
    }
    for src, dst in mapping.items():
        if src in props:
            new_props[dst] = props[src]

    if "Body" in props:
        new_props["DefinitionBody"] = props["Body"]
    if "BodyS3Location" in props:
        new_props["DefinitionUri"] = props["BodyS3Location"]

    new_res = OrderedDict(resource)
    new_res["Type"] = "AWS::Serverless::HttpApi"
    new_res["Properties"] = new_props
    return new_res


class _ApiDeps(NamedTuple):
    deployments: list[str]
    stages: list[str]
    blocking_refs: bool


def _collect_apigw_deps(resources: dict, rest_api_id: str) -> _ApiDeps:
    deployments: list[str] = []
    stages: list[str] = []
    blocking = False

    for lid, res in list(resources.items()):
        if not isinstance(res, (dict, OrderedDict)):
            continue
        res = OrderedDict(res)
        rtype = res.get("Type")
        if (
            rtype == "AWS::ApiGateway::Deployment"
            and _extract_logical_id(res.get("Properties", {}).get("RestApiId")) == rest_api_id
        ):
            deployments.append(lid)
        if rtype == "AWS::ApiGateway::Stage":
            stage_rest = _extract_logical_id(res.get("Properties", {}).get("RestApiId"))
            if stage_rest == rest_api_id:
                stages.append(lid)
            else:
                dep_ref = res.get("Properties", {}).get("DeploymentId")
                dep_id = _extract_logical_id(dep_ref)
                if dep_id in deployments:
                    blocking = True

        if rtype not in {
            "AWS::ApiGateway::Deployment",
            "AWS::ApiGateway::Stage",
            "AWS::ApiGateway::RestApi",
            "AWS::Serverless::Function",
            "AWS::Serverless::Api",
        }:
            if _references_any(res, deployments + stages + [rest_api_id]):
                blocking = True

    return _ApiDeps(deployments, stages, blocking)


def _references_any(resource: OrderedDict, logical_ids: list[str]) -> bool:
    targets = set(logical_ids)

    def visit(node) -> bool:
        if isinstance(node, CFNTag):
            return _extract_logical_id(node) in targets
        if isinstance(node, str):
            return node in targets
        if isinstance(node, (list, tuple)):
            return any(visit(item) for item in node)
        if isinstance(node, (dict, OrderedDict)):
            for value in node.values():
                if visit(value):
                    return True
        return False

    return visit(resource)


def _convert_rest_api(
    resource: OrderedDict,
    *,
    stage_name: Optional[str] = None,
    cors_config: Optional[OrderedDict] = None,
) -> Optional[OrderedDict]:
    props = resource.get("Properties")
    if not isinstance(props, (dict, OrderedDict)):
        return None

    new_props: OrderedDict = OrderedDict()
    mapping = {
        "Name": "Name",
        "Description": "Description",
        "FailOnWarnings": "FailOnWarnings",
        "EndpointConfiguration": "EndpointConfiguration",
        "BinaryMediaTypes": "BinaryMediaTypes",
        "MinimumCompressionSize": "MinimumCompressionSize",
        "AccessLogSetting": "AccessLogSetting",
        "CanarySetting": "CanarySetting",
        "Mode": "Mode",
        "ApiKeySourceType": "ApiKeySourceType",
        "Policy": "Policy",
        "OpenApiVersion": "OpenApiVersion",
        "Models": "Models",
        "Domain": "Domain",
        "AlwaysDeploy": "AlwaysDeploy",
        "PropagateTags": "PropagateTags",
        "Tags": "Tags",
    }
    for src, dst in mapping.items():
        if src in props:
            new_props[dst] = props[src]

    if "DefinitionBody" in props:
        new_props["DefinitionBody"] = props["DefinitionBody"]
    if "DefinitionUri" in props:
        new_props["DefinitionUri"] = props["DefinitionUri"]

    if isinstance(stage_name, str) and stage_name:
        new_props["StageName"] = stage_name

    if isinstance(cors_config, (dict, OrderedDict)) and cors_config:
        new_props["Cors"] = cors_config

    new_res = OrderedDict(resource)
    new_res["Type"] = "AWS::Serverless::Api"
    new_res["Properties"] = new_props
    return new_res


def _detect_cors_configuration(
    resources: dict,
    rest_api_id: str,
    path_cache: dict[str, str],
) -> tuple[Optional[OrderedDict], list[str], list[str]]:
    cors_methods: list[str] = []
    other_methods: list[str] = []

    allow_origin: Optional[str] = None
    allow_headers: Optional[str] = None
    allow_methods: Optional[str] = None
    has_root_cors = False

    for lid, res in list(resources.items()):
        if not isinstance(res, (dict, OrderedDict)):
            continue
        if res.get("Type") != "AWS::ApiGateway::Method":
            continue
        props = res.get("Properties") or {}
        if _extract_logical_id(props.get("RestApiId")) != rest_api_id:
            continue

        method = props.get("HttpMethod")
        method_str = str(method).upper() if isinstance(method, str) else ""
        integration = props.get("Integration")

        if method_str != "OPTIONS":
            other_methods.append(lid)
            continue

        cors_values = _extract_cors_from_integration(integration)
        if cors_values is None:
            other_methods.append(lid)
            continue

        origin_val, headers_val, methods_val = cors_values

        if allow_origin is None:
            allow_origin = origin_val
        elif allow_origin != origin_val:
            other_methods.append(lid)
            continue

        if headers_val is not None:
            if allow_headers is None:
                allow_headers = headers_val
            elif allow_headers != headers_val:
                other_methods.append(lid)
                continue

        if methods_val is not None:
            if allow_methods is None:
                allow_methods = methods_val
            elif allow_methods != methods_val:
                other_methods.append(lid)
                continue

        path = _resolve_method_path(props.get("ResourceId"), path_cache)
        if path == "/":
            has_root_cors = True

        cors_methods.append(lid)

    if not cors_methods:
        return None, [], other_methods

    if other_methods:
        return None, cors_methods, other_methods

    if not has_root_cors or allow_origin is None:
        return None, cors_methods, []

    cors_cfg: OrderedDict = OrderedDict()
    cors_cfg["AllowOrigin"] = allow_origin
    if allow_headers is not None:
        cors_cfg["AllowHeaders"] = allow_headers
    if allow_methods is not None:
        cors_cfg["AllowMethods"] = allow_methods

    return cors_cfg, cors_methods, []


def _extract_cors_from_integration(
    integration,
) -> Optional[tuple[str, Optional[str], Optional[str]]]:
    if not isinstance(integration, (dict, OrderedDict)):
        return None
    integration_type = integration.get("Type")
    if not (isinstance(integration_type, str) and integration_type.upper() == "MOCK"):
        return None
    responses = integration.get("IntegrationResponses")
    if not isinstance(responses, list) or not responses:
        return None
    first = responses[0]
    if not isinstance(first, (dict, OrderedDict)):
        return None
    params = first.get("ResponseParameters")
    if not isinstance(params, (dict, OrderedDict)):
        return None

    origin = params.get("method.response.header.Access-Control-Allow-Origin")
    headers = params.get("method.response.header.Access-Control-Allow-Headers")
    methods = params.get("method.response.header.Access-Control-Allow-Methods")

    if not isinstance(origin, str):
        return None
    if headers is not None and not isinstance(headers, str):
        return None
    if methods is not None and not isinstance(methods, str):
        return None

    return origin, headers, methods


def _collect_apigw_child_resources(resources: dict, rest_api_id: str) -> list[str]:
    child_ids: list[str] = []
    for lid, res in list(resources.items()):
        if not isinstance(res, (dict, OrderedDict)):
            continue
        if res.get("Type") != "AWS::ApiGateway::Resource":
            continue
        props = res.get("Properties") or {}
        if _extract_logical_id(props.get("RestApiId")) == rest_api_id:
            child_ids.append(lid)
    return child_ids


def _collect_apigw_permissions_for_api(resources: dict, rest_api_id: str) -> list[str]:
    perm_ids: list[str] = []
    for lid, res in list(resources.items()):
        if not isinstance(res, (dict, OrderedDict)):
            continue
        if res.get("Type") != "AWS::Lambda::Permission":
            continue
        props = res.get("Properties")
        if not isinstance(props, (dict, OrderedDict)):
            continue
        if props.get("Principal") != "apigateway.amazonaws.com":
            continue
        if not _source_arn_refers_to_api(props.get("SourceArn"), rest_api_id):
            continue
        perm_ids.append(lid)
    return perm_ids


def _select_stage_name(
    resources: dict,
    stage_ids: list[str],
    rest_api_id: str,
) -> Optional[str]:
    for stage_id in stage_ids:
        stage_res = resources.get(stage_id)
        if not isinstance(stage_res, (dict, OrderedDict)):
            continue
        if stage_res.get("Type") != "AWS::ApiGateway::Stage":
            continue
        props = stage_res.get("Properties") or {}
        if _extract_logical_id(props.get("RestApiId")) != rest_api_id:
            continue
        name = props.get("StageName")
        if isinstance(name, str) and name:
            return name
    return None


def _rewrite_stage_references(template: dict, stage_ids: list[str], stage_name: str) -> None:
    if not stage_ids or not isinstance(stage_name, str) or not stage_name:
        return

    def _replace_in_string(value: str) -> str:
        new_value = value
        for sid in stage_ids:
            placeholder = f"${{{sid}}}"
            if placeholder in new_value:
                new_value = new_value.replace(placeholder, stage_name)
        return new_value

    def visit(node) -> None:
        if isinstance(node, CFNTag):
            if node.tag == "Sub":
                inner = node.value
                if isinstance(inner, str):
                    node.value = _replace_in_string(inner)
                elif isinstance(inner, list) and inner and isinstance(inner[0], str):
                    inner[0] = _replace_in_string(inner[0])
            return

        if isinstance(node, (dict, OrderedDict)):
            for key, value in list(node.items()):
                if key == "Fn::Sub":
                    if isinstance(value, str):
                        node[key] = _replace_in_string(value)
                    elif isinstance(value, list) and value and isinstance(value[0], str):
                        value[0] = _replace_in_string(value[0])
                elif key == "Fn::Join":
                    join_val = value
                    if isinstance(join_val, list) and len(join_val) == 2:
                        items = join_val[1]
                        if isinstance(items, list):
                            for idx, part in enumerate(items):
                                if (
                                    isinstance(part, (dict, OrderedDict))
                                    and part.get("Ref") in stage_ids
                                ):
                                    items[idx] = stage_name
                                elif isinstance(part, str):
                                    items[idx] = _replace_in_string(part)
                    else:
                        visit(value)
                else:
                    visit(value)
            return

        if isinstance(node, list):
            for item in node:
                visit(item)

    visit(template)
