"""Conversion utilities for AWS AppSync resources."""

from __future__ import annotations

import re
from collections import OrderedDict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from ..formatter import CFNTag
from .asset_stager import SamAssetStager
from .function_converter import rewrite_function_url_refs
from .shared import (
    _ensure_sam_transform,
    _ensure_sub_tag,
    _extract_logical_id,
    _format_code_uri,
    _prepare_inline_code,
    _remove_resources,
    _sanitize_path,
)


def convert_appsync_apis(
    template: dict,
    *,
    asset_search_paths: Iterable[Path],
    relative_to: Optional[Path] = None,
    asset_stager: Optional[SamAssetStager] = None,
    prefer_external_assets: bool = False,
) -> Tuple[dict, bool]:
    """Collapse AWS::AppSync resources into AWS::Serverless::GraphQLApi."""

    resources = template.get("Resources")
    if not isinstance(resources, (dict, OrderedDict)):
        return template, False

    search_roots = [_sanitize_path(p) for p in asset_search_paths if p is not None]
    consumed_ids: List[str] = []
    changed = False
    reference_updates: Dict[str, str] = {}

    api_key_blocks: List[OrderedDict] = []

    for logical_id, resource in list(resources.items()):
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::AppSync::GraphQLApi":
            continue
        converted = _convert_single_appsync_api(
            logical_id,
            resource,
            resources,
            search_roots,
            relative_to,
            asset_stager,
            prefer_external_assets,
        )
        if converted is None:
            continue
        new_resource, removed_ids, rename_map, api_keys_block = converted
        resources[logical_id] = new_resource
        consumed_ids.extend(removed_ids)
        changed = True
        if rename_map:
            reference_updates.update(rename_map)
        if api_keys_block:
            api_key_blocks.append(api_keys_block)

    _remove_resources(template, consumed_ids)
    if reference_updates:
        reference_updates = _expand_rename_map_with_strings(template, reference_updates)
        for block in api_key_blocks:
            _rewrite_api_key_string_refs(template, block)
        rewrite_function_url_refs(
            template,
            reference_updates,
            rewrite_literals=False,
            literal_key_allowlist={"DependsOn"},
        )
    if changed:
        _ensure_sam_transform(template)
    return template, changed


def _convert_single_appsync_api(
    api_logical_id: str,
    api_resource: dict,
    resources: dict,
    search_roots: List[Path],
    relative_to: Optional[Path],
    asset_stager: Optional[SamAssetStager],
    prefer_external_assets: bool,
) -> Optional[Tuple[dict, List[str], Dict[str, str], Optional[OrderedDict]]]:
    properties = api_resource.get("Properties")
    if not isinstance(properties, (dict, OrderedDict)):
        return None

    schema_id, schema_resource = _find_graphql_schema(resources, api_logical_id)
    if not schema_resource:
        return None

    schema_props = schema_resource.get("Properties") or {}
    schema_inline = schema_props.get("Definition")
    schema_uri = schema_props.get("DefinitionS3Location")
    if schema_inline is None and schema_uri is None:
        return None

    data_sources_block, data_source_lookup, ds_consumed = _collect_graphql_data_sources(
        resources,
        api_logical_id,
    )
    if data_sources_block is None:
        return None

    functions_block, function_lookup, fn_consumed = _collect_graphql_functions(
        resources,
        api_logical_id,
        data_source_lookup,
        search_roots,
        relative_to,
        asset_stager,
        prefer_external_assets,
    )
    if not functions_block:
        return None

    resolvers_block, resolver_consumed = _collect_graphql_resolvers(
        resources,
        api_logical_id,
        function_lookup,
        search_roots,
        relative_to,
        asset_stager,
        prefer_external_assets,
    )
    if not resolvers_block:
        return None

    api_keys_block, key_consumed, api_key_reference_map = _collect_graphql_api_keys(
        resources, api_logical_id
    )

    auth_block = _build_auth_block(properties)
    if auth_block is None:
        return None

    new_properties = OrderedDict()
    new_properties["Auth"] = auth_block
    if properties.get("Name") is not None:
        new_properties["Name"] = properties["Name"]
    if schema_inline is not None:
        prepared_schema = _prepare_inline_code(schema_inline)
        if prefer_external_assets and asset_stager is not None:
            schema_logical = schema_id or f"{api_logical_id}Schema"
            staged_schema = asset_stager.stage_inline_text(
                schema_logical,
                str(prepared_schema),
                file_name="schema.graphql",
            )
            new_properties["SchemaUri"] = _format_code_uri(staged_schema, relative_to)
        else:
            new_properties["SchemaInline"] = prepared_schema
    elif schema_uri is not None:
        new_properties["SchemaUri"] = schema_uri
    new_properties["DataSources"] = data_sources_block
    new_properties["Functions"] = functions_block
    new_properties["Resolvers"] = resolvers_block
    if api_keys_block:
        new_properties["ApiKeys"] = api_keys_block
    if properties.get("LogConfig") is not None:
        new_properties["Logging"] = properties["LogConfig"]
    if properties.get("XrayEnabled") is not None:
        new_properties["XrayEnabled"] = properties["XrayEnabled"]
    if properties.get("Tags") is not None:
        new_properties["Tags"] = properties["Tags"]
    if properties.get("Cache") is not None:
        new_properties["Cache"] = properties["Cache"]
    if properties.get("DomainName") is not None:
        new_properties["DomainName"] = properties["DomainName"]

    # Mutate the existing GraphQLApi resource so that any comments attached
    # to the logical ID or Type line are preserved on the SAM GraphQLApi.
    api_resource["Type"] = "AWS::Serverless::GraphQLApi"
    api_resource["Properties"] = new_properties

    consumed_ids = [
        entry
        for entry in [schema_id, *ds_consumed, *fn_consumed, *resolver_consumed, *key_consumed]
        if entry
    ]
    return api_resource, consumed_ids, api_key_reference_map, api_keys_block


def _find_graphql_schema(
    resources: dict, api_logical_id: str
) -> Tuple[Optional[str], Optional[OrderedDict]]:
    for logical_id, resource in resources.items():
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::AppSync::GraphQLSchema":
            continue
        props = resource.get("Properties")
        if not isinstance(props, (dict, OrderedDict)):
            continue
        api_ref = props.get("ApiId")
        if _extract_logical_id(api_ref) != api_logical_id:
            continue
        return logical_id, resource
    return None, None


def _collect_graphql_data_sources(
    resources: dict,
    api_logical_id: str,
) -> Tuple[Optional[OrderedDict], Dict[str, str], List[str]]:
    dynamodb_entries: OrderedDict[str, OrderedDict] = OrderedDict()
    lambda_entries: OrderedDict[str, OrderedDict] = OrderedDict()
    name_lookup: Dict[str, str] = {}
    consumed: List[str] = []

    for logical_id, resource in resources.items():
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::AppSync::DataSource":
            continue
        props = resource.get("Properties")
        if not isinstance(props, (dict, OrderedDict)):
            continue
        api_ref = props.get("ApiId")
        if _extract_logical_id(api_ref) != api_logical_id:
            continue
        entry_key = props.get("Name") if isinstance(props.get("Name"), str) else logical_id
        ds_type = props.get("Type")
        if ds_type == "AMAZON_DYNAMODB":
            entry = OrderedDict()
            config = props.get("DynamoDBConfig") or {}
            if "TableName" in config:
                entry["TableName"] = config["TableName"]
            if "AwsRegion" in config:
                entry["Region"] = config["AwsRegion"]
            if "DeltaSyncConfig" in config:
                entry["DeltaSync"] = config["DeltaSyncConfig"]
            if "UseCallerCredentials" in config:
                entry["UseCallerCredentials"] = config["UseCallerCredentials"]
            if "Versioned" in config:
                entry["Versioned"] = config["Versioned"]
            if props.get("ServiceRoleArn") is not None:
                entry["ServiceRoleArn"] = props["ServiceRoleArn"]
            if props.get("Description") is not None:
                entry["Description"] = props["Description"]
            if props.get("Name") is not None:
                entry["Name"] = props["Name"]
            dynamodb_entries[entry_key] = entry
        elif ds_type == "AWS_LAMBDA":
            entry = OrderedDict()
            lambda_config = props.get("LambdaConfig") or {}
            if lambda_config.get("LambdaFunctionArn") is not None:
                entry["FunctionArn"] = lambda_config["LambdaFunctionArn"]
            if props.get("ServiceRoleArn") is not None:
                entry["ServiceRoleArn"] = props["ServiceRoleArn"]
            if props.get("Description") is not None:
                entry["Description"] = props["Description"]
            if props.get("Name") is not None:
                entry["Name"] = props["Name"]
            lambda_entries[entry_key] = entry
        else:
            return None, {}, []

        friendly = props.get("Name")
        if isinstance(friendly, str):
            name_lookup[friendly] = entry_key
        name_lookup[logical_id] = entry_key
        consumed.append(logical_id)

    block = OrderedDict()
    if dynamodb_entries:
        block["DynamoDb"] = dynamodb_entries
    if lambda_entries:
        block["Lambda"] = lambda_entries

    if not block:
        return None, {}, []

    return block, name_lookup, consumed


def _collect_graphql_functions(
    resources: dict,
    api_logical_id: str,
    data_source_lookup: Dict[str, str],
    search_roots: List[Path],
    relative_to: Optional[Path],
    asset_stager: Optional[SamAssetStager],
    prefer_external_assets: bool,
) -> Tuple[OrderedDict, Dict[str, str], List[str]]:
    entries: OrderedDict[str, OrderedDict] = OrderedDict()
    lookup: Dict[str, str] = {}
    consumed: List[str] = []

    for logical_id, resource in resources.items():
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::AppSync::FunctionConfiguration":
            continue
        props = resource.get("Properties")
        if not isinstance(props, (dict, OrderedDict)):
            continue
        if _extract_logical_id(props.get("ApiId")) != api_logical_id:
            continue

        entry = OrderedDict()
        runtime = _normalize_graphql_runtime(props.get("Runtime"))
        if runtime is not None:
            entry["Runtime"] = runtime
        data_source_name = props.get("DataSourceName")
        if isinstance(data_source_name, str):
            entry["DataSource"] = data_source_lookup.get(data_source_name, data_source_name)
        if props.get("Description") is not None:
            entry["Description"] = props["Description"]
        if props.get("Name") is not None:
            entry["Name"] = props["Name"]
        if props.get("MaxBatchSize") is not None:
            entry["MaxBatchSize"] = props["MaxBatchSize"]
        if props.get("SyncConfig") is not None:
            entry["Sync"] = props["SyncConfig"]

        code_uri, inline_code = _resolve_appsync_code_asset(
            logical_id,
            props,
            search_roots=search_roots,
            relative_to=relative_to,
            asset_stager=asset_stager,
            preferred_name=_infer_code_filename(props, default_name="function"),
            prefer_external_assets=prefer_external_assets,
        )
        if inline_code is not None:
            entry["InlineCode"] = inline_code
        if code_uri is not None:
            entry["CodeUri"] = code_uri

        entries[logical_id] = entry
        lookup[logical_id] = logical_id
        consumed.append(logical_id)

    return entries, lookup, consumed


def _collect_graphql_resolvers(
    resources: dict,
    api_logical_id: str,
    function_lookup: Dict[str, str],
    search_roots: List[Path],
    relative_to: Optional[Path],
    asset_stager: Optional[SamAssetStager],
    prefer_external_assets: bool,
) -> Tuple[OrderedDict, List[str]]:
    grouped: OrderedDict[str, OrderedDict] = OrderedDict()
    consumed: List[str] = []

    for logical_id, resource in resources.items():
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::AppSync::Resolver":
            continue
        props = resource.get("Properties")
        if not isinstance(props, (dict, OrderedDict)):
            continue
        if _extract_logical_id(props.get("ApiId")) != api_logical_id:
            continue
        if props.get("Kind") not in (None, "PIPELINE"):
            return OrderedDict(), []

        operation = props.get("TypeName")
        if not isinstance(operation, str) or not operation:
            return OrderedDict(), []
        field_name = props.get("FieldName")
        if not isinstance(field_name, str) or not field_name:
            return OrderedDict(), []

        pipeline_refs = (
            props.get("PipelineConfig", {}).get("Functions")
            if isinstance(props.get("PipelineConfig"), (dict, OrderedDict))
            else None
        )
        if not pipeline_refs:
            return OrderedDict(), []
        pipeline: List[str] = []
        for ref in pipeline_refs:
            target = _extract_logical_id(ref)
            if not target:
                return OrderedDict(), []
            pipeline.append(function_lookup.get(target, target))

        entry = OrderedDict()
        entry["FieldName"] = field_name
        entry["Pipeline"] = pipeline
        runtime = _normalize_graphql_runtime(props.get("Runtime"))
        if runtime is not None:
            entry["Runtime"] = runtime
        if props.get("MaxBatchSize") is not None:
            entry["MaxBatchSize"] = props["MaxBatchSize"]
        if props.get("SyncConfig") is not None:
            entry["Sync"] = props["SyncConfig"]
        if props.get("CachingConfig") is not None:
            entry["Caching"] = props["CachingConfig"]

        code_uri, inline_code = _resolve_appsync_code_asset(
            logical_id,
            props,
            search_roots=search_roots,
            relative_to=relative_to,
            asset_stager=asset_stager,
            preferred_name=_infer_code_filename(props, default_name="resolver"),
            prefer_external_assets=prefer_external_assets,
        )
        if inline_code is not None:
            entry["InlineCode"] = inline_code
        if code_uri is not None:
            entry["CodeUri"] = code_uri

        target_group = grouped.setdefault(operation, OrderedDict())
        target_group[logical_id] = entry
        consumed.append(logical_id)

    return grouped, consumed


def _collect_graphql_api_keys(
    resources: dict, api_logical_id: str
) -> Tuple[OrderedDict, List[str], Dict[str, str]]:
    entries: OrderedDict[str, OrderedDict] = OrderedDict()
    consumed: List[str] = []
    rename_map: Dict[str, str] = {}

    for logical_id, resource in resources.items():
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::AppSync::ApiKey":
            continue
        props = resource.get("Properties")
        if not isinstance(props, (dict, OrderedDict)):
            continue
        if _extract_logical_id(props.get("ApiId")) != api_logical_id:
            continue

        entry = OrderedDict()
        if props.get("Description") is not None:
            entry["Description"] = props["Description"]
        if props.get("Expires") is not None:
            entry["ExpiresOn"] = props["Expires"]
        entry["ApiKeyId"] = props.get("ApiKeyId") or logical_id
        entries[logical_id] = entry
        consumed.append(logical_id)

        # SAM generates ApiKey logical IDs by prefixing the parent GraphQLApi logical ID.
        generated_id = f"{api_logical_id}{logical_id}"

        rename_map[logical_id] = generated_id
        rename_map[generated_id] = generated_id

        original_api = _extract_logical_id(props.get("ApiId"))
        if original_api:
            rename_map[f"{original_api}{logical_id}"] = generated_id

    return entries, consumed, rename_map


def _expand_rename_map_with_strings(template: dict, rename_map: Dict[str, str]) -> Dict[str, str]:
    if not rename_map:
        return rename_map

    strings: set[str] = set()

    def visit(node):
        if isinstance(node, str):
            strings.add(node)
            return
        if isinstance(node, (list, tuple)):
            for item in node:
                visit(item)
        elif isinstance(node, (dict, OrderedDict)):
            for value in node.values():
                visit(value)

    visit(template)

    expanded = dict(rename_map)
    for candidate in strings:
        for key, new in rename_map.items():
            if key != candidate and candidate.endswith(key):
                expanded[candidate] = new
    return expanded


def _rewrite_api_key_string_refs(template: dict, api_keys_block: Optional[OrderedDict]) -> None:
    if not api_keys_block:
        return
    keys = [k for k in api_keys_block.keys() if isinstance(k, str)]
    if not keys:
        return
    rename_map: Dict[str, str] = {}

    strings: set[str] = set()

    def visit(node):
        if isinstance(node, str):
            strings.add(node)
            return
        if isinstance(node, (list, tuple)):
            for item in node:
                visit(item)
        elif isinstance(node, (dict, OrderedDict)):
            for value in node.values():
                visit(value)

    visit(template)

    for candidate in strings:
        for key in keys:
            if key in candidate and candidate != key:
                rename_map[candidate] = key

    if rename_map:
        rewrite_function_url_refs(
            template, rename_map, rewrite_literals=True, literal_key_allowlist={"DependsOn"}
        )


def _build_auth_block(properties: OrderedDict) -> Optional[OrderedDict]:
    auth_type = properties.get("AuthenticationType")
    if not isinstance(auth_type, str):
        return None
    auth = OrderedDict()
    auth["Type"] = auth_type

    mapping = {
        "OpenIDConnectConfig": "OpenIDConnect",
        "UserPoolConfig": "UserPool",
        "LambdaAuthorizerConfig": "LambdaAuthorizer",
    }
    for source, target in mapping.items():
        if properties.get(source) is not None:
            auth[target] = properties[source]

    additional = []
    providers = properties.get("AdditionalAuthenticationProviders")
    if isinstance(providers, list):
        for provider in providers:
            if not isinstance(provider, (dict, OrderedDict)):
                continue
            entry = OrderedDict()
            provider_type = provider.get("AuthenticationType")
            if not isinstance(provider_type, str):
                continue
            entry["Type"] = provider_type
            if provider.get("LambdaAuthorizerConfig") is not None:
                entry["LambdaAuthorizer"] = provider["LambdaAuthorizerConfig"]
            if provider.get("OpenIDConnectConfig") is not None:
                entry["OpenIDConnect"] = provider["OpenIDConnectConfig"]
            if provider.get("UserPoolConfig") is not None:
                entry["UserPool"] = provider["UserPoolConfig"]
            additional.append(entry)
    if additional:
        auth["Additional"] = additional

    return auth


def _resolve_appsync_code_asset(
    logical_id: str,
    props: OrderedDict,
    *,
    search_roots: List[Path],
    relative_to: Optional[Path],
    asset_stager: Optional[SamAssetStager],
    preferred_name: str,
    prefer_external_assets: bool,
) -> Tuple[Optional[str], Optional[str]]:
    inline_value = props.get("Code")
    if isinstance(inline_value, str):
        prepared = _prepare_inline_code(inline_value)
        if prefer_external_assets and asset_stager is not None:
            staged = asset_stager.stage_inline_text(
                logical_id,
                str(prepared),
                file_name=preferred_name,
            )
            return _format_code_uri(staged, relative_to), None
        return None, prepared

    location = props.get("CodeS3Location")
    if location is None:
        return None, None

    resolved_location = _resolve_s3_location_string(location, asset_stager)

    if asset_stager is not None:
        lookup_value = resolved_location or location
        local_asset = _find_local_appsync_asset(lookup_value, search_roots)
        if local_asset is not None:
            staged = asset_stager.stage_file_asset(
                logical_id,
                local_asset,
                file_name=_determine_filename(local_asset, preferred_name),
            )
            return _format_code_uri(staged, relative_to), None
        bucket, key = _parse_literal_s3_uri(resolved_location or lookup_value)
        if bucket and key:
            staged = asset_stager.stage_s3_file(
                logical_id,
                bucket,
                key,
                file_name=_determine_filename(Path(key), preferred_name),
            )
            return _format_code_uri(staged, relative_to), None

    return resolved_location or location, None


def _normalize_graphql_runtime(value) -> Optional[OrderedDict]:
    if not isinstance(value, (dict, OrderedDict)):
        return None
    runtime = OrderedDict()
    if value.get("Name") is not None:
        runtime["Name"] = value["Name"]
    version = value.get("Version") or value.get("RuntimeVersion")
    if version is not None:
        runtime["Version"] = version
    return runtime if runtime else None


def _infer_code_filename(props: OrderedDict, default_name: str) -> str:
    suffix = ".js"
    location = props.get("CodeS3Location")
    candidate = _stringify_s3_location(location)
    if candidate:
        suffix = Path(candidate).suffix or suffix
    return f"{default_name}{suffix}"


def _determine_filename(source_path: Path, preferred_name: str) -> str:
    suffix = source_path.suffix
    if suffix:
        if preferred_name.endswith(suffix):
            return preferred_name
        return f"{preferred_name}{suffix}"
    return preferred_name


def _stringify_s3_location(value) -> Optional[str]:
    if isinstance(value, str):
        return value
    if isinstance(value, CFNTag) and value.tag == "Sub":
        template = value.value[0] if isinstance(value.value, list) else value.value
        if isinstance(template, str):
            return template
    if isinstance(value, dict):
        if "Fn::Sub" in value:
            template = value["Fn::Sub"]
            if isinstance(template, list) and template:
                template = template[0]
            if isinstance(template, str):
                return template
    return None


def _resolve_s3_location_string(value, asset_stager: Optional[SamAssetStager]) -> Optional[str]:
    literal = _stringify_s3_location(value)
    if literal and "${" not in literal:
        return literal
    if asset_stager is not None:
        resolved = asset_stager.resolve_string(_ensure_sub_tag(value))
        if isinstance(resolved, str):
            return resolved
    return literal


def _parse_literal_s3_uri(value) -> Tuple[Optional[str], Optional[str]]:
    string = value if isinstance(value, str) else _stringify_s3_location(value)
    if not string or not string.lower().startswith("s3://"):
        return None, None
    remainder = string[5:]
    if "/" not in remainder:
        return None, None
    bucket, key = remainder.split("/", 1)
    if not bucket or not key or "${" in bucket or "${" in key:
        return None, None
    return bucket, key


def _find_local_appsync_asset(value, search_roots: List[Path]) -> Optional[Path]:
    if isinstance(value, str):
        string = value
    else:
        string = _stringify_s3_location(value)
    if not string or not string.lower().startswith("s3://"):
        return None
    parts = string.split("/", 3)
    if len(parts) < 4:
        return None
    key = parts[3]
    base_name = Path(key).name
    if not base_name:
        return None

    candidates = [base_name]
    if not base_name.startswith("asset."):
        candidates.append(f"asset.{base_name}")
    hash_part = _extract_asset_hash(base_name)
    if hash_part and not base_name.startswith("asset."):
        suffix = Path(base_name).suffix
        candidates.append(f"asset.{hash_part}{suffix}")

    for root in search_roots:
        for candidate in candidates:
            candidate_path = root / candidate
            try:
                if candidate_path.exists():
                    return candidate_path
            except OSError:
                continue
    return None


def _extract_asset_hash(name: str) -> Optional[str]:
    match = re.search(r"([0-9a-f]{32,64})", name)
    if match:
        return match.group(1)
    return None
