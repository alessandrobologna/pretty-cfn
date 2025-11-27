"""CDK template cleaner scaffolding.

This module provides a first pass at cleaning CDK-synthesized CloudFormation
templates. It focuses on:
- Detecting and stripping 8-hex hash suffixes from logical IDs
- Deterministic collision resolution
- Updating common reference forms (!Ref, !GetAtt, !Sub)
- Optionally removing AWS::CDK::Metadata resources and aws:asset metadata

Notes
-----
- In deployable mode for existing stacks, logical ID renames are off by default
  to avoid replacements.
"""

from __future__ import annotations

import copy
import re
from pathlib import Path
from typing import Dict, List, Union, Optional, Any


from .formatter import CFNTag
from .cdk_metadata import CDKMetadataLoader


HASH_PATTERN = re.compile(r"[A-F0-9]{8}$")
SUB_TOKEN_RE = re.compile(r"\$\{([^}]+)\}")


def is_cdk_hash(name: str) -> bool:
    return bool(HASH_PATTERN.search(name))


def strip_hash_suffix(name: str) -> str:
    return name[:-8] if is_cdk_hash(name) else name


class CDKCleaner:
    def __init__(
        self,
        mode: str = "readable",
        *,
        strip_hashes: bool | None = None,
        semantic_naming: bool | None = None,
        remove_cdk_metadata: bool | None = None,
        keep_path_metadata: bool | None = None,
        strip_asset_metadata: bool | None = None,
        collision_strategy: str | None = None,  # 'numbered' | 'short-hash'
        rename_logical_ids: bool | None = None,
        cdk_metadata: Optional[Union[str, Path, Dict[str, Any]]] = None,
    ) -> None:
        self.mode = mode
        readable = mode == "readable"
        # Defaults by mode
        self.strip_hashes = True if strip_hashes is None else strip_hashes
        self.semantic_naming = True if semantic_naming is None else semantic_naming
        self.remove_cdk_metadata = (
            (True if readable else False) if remove_cdk_metadata is None else remove_cdk_metadata
        )
        self.keep_path_metadata = True if keep_path_metadata is None else keep_path_metadata
        self.strip_asset_metadata = (
            (True if readable else False) if strip_asset_metadata is None else strip_asset_metadata
        )
        self.collision_strategy = "numbered" if collision_strategy is None else collision_strategy
        # Renaming logical IDs is dangerous in existing stacks; default off in deployable
        self.rename_logical_ids = (
            (True if readable else False) if rename_logical_ids is None else rename_logical_ids
        )

        # Load CDK metadata if provided
        self.cdk_mappings: Dict[str, Any] = {}
        if cdk_metadata:
            if isinstance(cdk_metadata, dict):
                self.cdk_mappings = cdk_metadata
            else:
                try:
                    self.cdk_mappings = CDKMetadataLoader.load(cdk_metadata)
                except Exception:
                    # Silently fall back to pattern matching
                    pass

    # Public API
    def clean(self, template: dict) -> dict:
        data = copy.deepcopy(template)
        self.last_mapping: Dict[str, str] = {}

        # Clean up Lambda ZipFile content (trim trailing whitespace for better formatting)
        data = self._clean_zipfile_content(data)

        # Remove AWS::CDK::Metadata resources first (optional)
        if self.remove_cdk_metadata:
            data = self._remove_cdk_metadata(data)

        # Strip aws:asset:* metadata in readable mode
        if self.strip_asset_metadata:
            data = self._strip_asset_metadata(data)

        # Clean CDK v1 style asset parameters in readable mode
        if self.rename_logical_ids:
            data = self._clean_asset_parameters(data)

        # Compute renames
        name_mapping: Dict[str, str] = {}
        if self.rename_logical_ids:
            name_mapping = self._compute_name_mapping(data.get("Resources", {}))
            self.last_mapping = dict(name_mapping)
            if name_mapping:
                # Update references across the template
                data = self._update_references(data, name_mapping)
                # Rename the resource keys
                data = self._rename_resource_keys(data, name_mapping)

        # If we removed CDK metadata, also remove CDKMetadataAvailable condition (common CDK pattern)
        if self.remove_cdk_metadata:
            data = self._remove_cdk_condition(data, "CDKMetadataAvailable")

        return data

    def get_rename_map(self) -> Dict[str, str]:
        return getattr(self, "last_mapping", {})

    # Internal helpers
    def _compute_name_mapping(self, resources: Dict[str, dict]) -> Dict[str, str]:
        # Derive base names using aws:cdk:path when available; apply semantics; optionally strip hash
        derived: Dict[str, str] = {}
        for old_name, body in resources.items():
            md = body.get("Metadata") if isinstance(body, dict) else None
            base = self._derive_base_name(old_name, md)
            if self.strip_hashes:
                base = strip_hash_suffix(base)
            if self.semantic_naming:
                base = self._apply_semantics(base)
            derived[old_name] = base

        # Collision detection
        groups: Dict[str, List[str]] = {}
        for old, base in derived.items():
            groups.setdefault(base, []).append(old)

        mapping: Dict[str, str] = {}
        for base, olds in groups.items():
            olds_sorted = sorted(olds)
            if len(olds_sorted) == 1:
                mapping[olds_sorted[0]] = base
            else:
                for idx, old in enumerate(olds_sorted, start=1):
                    if idx == 1:
                        mapping[old] = base
                    else:
                        if self.collision_strategy == "short-hash":
                            suffix = self._short_hash(old)
                            mapping[old] = f"{base}{suffix}"
                        else:
                            mapping[old] = f"{base}{idx}"
        return mapping

    def _rename_resource_keys(self, template: dict, mapping: Dict[str, str]) -> dict:
        res = template.get("Resources") or {}
        if not isinstance(res, dict) or not mapping:
            return template

        # Work on a deep copy of the template so we don't mutate callers' data,
        # but preserve the original mapping type (CommentedMap vs plain dict)
        out = copy.deepcopy(template)
        out_res = out.get("Resources") or {}
        if not isinstance(out_res, dict):
            return out

        # Preserve any top-level comment on the Resources mapping
        if hasattr(out_res, "ca") and getattr(out_res.ca, "comment", None):
            top_comment = out_res.ca.comment
        else:
            top_comment = None

        res_type = out_res.__class__
        new_res: Dict[str, dict] = res_type()
        if top_comment is not None and hasattr(new_res, "ca"):
            new_res.ca.comment = top_comment

        for old_name, body in out_res.items():
            new_name = mapping.get(old_name, old_name)
            # Update DependsOn inside the resource while we're here
            if isinstance(body, dict) and "DependsOn" in body:
                body = copy.deepcopy(body)
                dep = body["DependsOn"]
                if isinstance(dep, str):
                    body["DependsOn"] = mapping.get(dep, dep)
                elif isinstance(dep, list):
                    body["DependsOn"] = [mapping.get(x, x) for x in dep]
            new_res[new_name] = body
            # Preserve any comments attached to the original logical ID key
            if hasattr(out_res, "ca") and hasattr(new_res, "ca"):
                items = getattr(out_res.ca, "items", {})
                if items and old_name in items:
                    new_res.ca.items[new_name] = items[old_name]

        out["Resources"] = new_res
        return out

    def _update_references(
        self, obj: Union[dict, list, CFNTag, str, int, float, None], mapping: Dict[str, str]
    ):
        if isinstance(obj, dict):
            # Handle long-form intrinsics in JSON/YAML
            if set(obj.keys()) == {"Ref"}:
                val = obj["Ref"]
                if isinstance(val, str):
                    obj["Ref"] = mapping.get(val, val)
                return obj
            if set(obj.keys()) == {"Fn::GetAtt"}:
                val = obj["Fn::GetAtt"]
                if isinstance(val, list) and val:
                    name = val[0]
                    if isinstance(name, str):
                        obj["Fn::GetAtt"] = [mapping.get(name, name)] + list(val[1:])
                        return obj
                if isinstance(val, str) and "." in val:
                    name, rest = val.split(".", 1)
                    name = mapping.get(name, name)
                    obj["Fn::GetAtt"] = f"{name}.{rest}"
                    return obj
            if set(obj.keys()) == {"Fn::Sub"}:
                val = obj["Fn::Sub"]
                if isinstance(val, str):
                    obj["Fn::Sub"] = _replace_sub_tokens(val, mapping)
                    return obj
                if isinstance(val, list) and val:
                    s = val[0]
                    rest = val[1:]
                    if isinstance(s, str):
                        s = _replace_sub_tokens(s, mapping)
                    rest = [self._update_references(r, mapping) for r in rest]
                    obj["Fn::Sub"] = [s] + rest
                    return obj
            if set(obj.keys()) == {"Fn::ImportValue"}:
                val = obj["Fn::ImportValue"]
                if isinstance(val, str):
                    # Try to replace tokens inside ${...}
                    obj["Fn::ImportValue"] = _replace_sub_tokens(val, mapping)
                    return obj
                if isinstance(val, dict) or isinstance(val, list):
                    obj["Fn::ImportValue"] = self._update_references(val, mapping)
                    return obj
            # Default recursion for generic dicts
            for k, v in list(obj.items()):
                obj[k] = self._update_references(v, mapping)
            return obj
        if isinstance(obj, list):
            for idx, item in enumerate(obj):
                obj[idx] = self._update_references(item, mapping)
            return obj
        if isinstance(obj, CFNTag):
            tag = obj.tag
            val = obj.value
            if tag == "Ref" and isinstance(val, str):
                obj.value = mapping.get(val, val)
                return obj
            if tag == "GetAtt":
                if isinstance(val, str) and "." in val:
                    name, rest = val.split(".", 1)
                    name = mapping.get(name, name)
                    obj.value = f"{name}.{rest}"
                    return obj
                if isinstance(val, list) and val:
                    name = val[0]
                    if isinstance(name, str):
                        new = mapping.get(name, name)
                        obj.value = [new] + list(val[1:])
                        return obj
            if tag == "Sub":
                if isinstance(val, str):
                    obj.value = _replace_sub_tokens(val, mapping)
                    return obj
                if isinstance(val, list) and val:
                    s = val[0]
                    rest = val[1:]
                    if isinstance(s, str):
                        s = _replace_sub_tokens(s, mapping)
                    # Also walk the mapping entries if present
                    rest = [self._update_references(r, mapping) for r in rest]
                    obj.value = [s] + rest
                    return obj
            # Default: recurse into the tag payload to update nested references
            if isinstance(val, (dict, list, CFNTag)):
                obj.value = self._update_references(val, mapping)
            return obj
        # Primitive types
        return obj

    def _remove_cdk_metadata(self, template: dict) -> dict:
        res = template.get("Resources")
        if not isinstance(res, dict):
            return template
        out = copy.deepcopy(template)
        out_res = out.get("Resources")
        if not isinstance(out_res, dict):
            return out
        for name, body in list(out_res.items()):
            if isinstance(body, dict) and body.get("Type") == "AWS::CDK::Metadata":
                out_res.pop(name, None)
        return out

    def _strip_asset_metadata(self, template: dict) -> dict:
        res = template.get("Resources")
        if not isinstance(res, dict):
            return template
        out = copy.deepcopy(template)
        for _, body in out["Resources"].items():
            if isinstance(body, dict) and isinstance(body.get("Metadata"), dict):
                md = body["Metadata"]
                for k in list(md.keys()):
                    if isinstance(k, str):
                        if k.startswith("aws:asset") or k.startswith("aws:cdk:asset"):
                            md.pop(k, None)
                        if k == "aws:cdk:path" and not self.keep_path_metadata:
                            md.pop(k, None)
        return out

    def _remove_cdk_condition(self, template: dict, cond_name: str) -> dict:
        conds = template.get("Conditions")
        if isinstance(conds, dict) and cond_name in conds:
            out = copy.deepcopy(template)
            out["Conditions"].pop(cond_name, None)
            return out
        return template

    def _clean_asset_parameters(self, template: dict) -> dict:
        # Only in readable (rename) mode; remove v1 AssetParameters and replace references with placeholders
        params = template.get("Parameters")
        if not isinstance(params, dict):
            return template
        to_remove = [
            p for p in params.keys() if isinstance(p, str) and p.startswith("AssetParameters")
        ]
        if not to_remove:
            return template

        out = copy.deepcopy(template)
        # Remove parameters
        for p in to_remove:
            out["Parameters"].pop(p, None)

        # Build replacement map by suffix
        def placeholder(name: str) -> str:
            if name.endswith("S3Bucket"):
                return "<asset-bucket>"
            if name.endswith("S3VersionKey"):
                return "<asset-key>"
            if name.endswith("ArtifactHash"):
                return "<asset-hash>"
            return "<asset-param>"

        repl: Dict[str, str] = {p: placeholder(p) for p in to_remove}

        # Walk and replace {Ref: AssetParameters...} and CFNTag Ref
        def replace_refs(o):
            if isinstance(o, dict):
                if set(o.keys()) == {"Ref"} and o["Ref"] in repl:
                    return repl[o["Ref"]]
                for k, v in list(o.items()):
                    o[k] = replace_refs(v)
                return o
            if isinstance(o, list):
                for idx, item in enumerate(o):
                    o[idx] = replace_refs(item)
                return o
            if (
                isinstance(o, CFNTag)
                and o.tag == "Ref"
                and isinstance(o.value, str)
                and o.value in repl
            ):
                return repl[o.value]
            return o

        replace_refs(out)
        return out

    def _derive_base_name(self, old_name: str, metadata: Optional[dict]) -> str:
        # First check if we have CDK metadata with exact mappings
        if old_name in self.cdk_mappings:
            mapping_info = self.cdk_mappings[old_name]
            construct_name = mapping_info.get("construct_name", "")
            if construct_name:
                # For generated resources, we might want to simplify further
                if mapping_info.get("is_generated", False):
                    # Remove redundant suffixes for generated resources
                    construct_name = self._simplify_generated_name(construct_name)
                # Ensure the name is valid CloudFormation logical ID
                return self._sanitize_logical_id(construct_name)

        # Fall back to using the original name with improved semantics
        # IMPORTANT: We should NOT create new names from path components
        # The path is for understanding context, not creating resource names
        base = old_name

        # Use metadata to understand the resource type and apply semantic improvements
        if isinstance(metadata, dict) and isinstance(metadata.get("aws:cdk:path"), str):
            path = metadata.get("aws:cdk:path")
            parts = path.split("/")

            # Use the path to understand what kind of resource this is
            # but keep the original resource name as the base
            if any("Lambda" in p or "Function" in p for p in parts):
                # It's a Lambda-related resource, keep original name but can apply semantics
                pass
            elif any("ApiGateway" in p or "Api" in p for p in parts):
                # API Gateway resource - apply appropriate semantics
                if "proxy" in path.lower() and "resource" in path.lower():
                    # This is the proxy resource, give it a clear name
                    base = "ApiGatewayProxyResource"
                elif any("permission" in p.lower() for p in parts):
                    # Permission resources - extract method if available
                    method = None
                    for part in parts:
                        if part in ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "ANY"]:
                            method = part
                            break
                    if method:
                        base = f"ApiGateway{method}Permission"
            # For other resources, keep the original name

        # Ensure valid CloudFormation logical ID
        return self._sanitize_logical_id(base)

    def _sanitize_logical_id(self, name: str) -> str:
        """
        Ensure a resource name is a valid CloudFormation logical ID.
        CloudFormation logical IDs must:
        - Contain only alphanumeric characters (A-Z, a-z, 0-9)
        - Start with a letter
        """
        # Remove all non-alphanumeric characters
        cleaned = "".join(c for c in name if c.isalnum())

        # Ensure it starts with a letter
        if cleaned and not cleaned[0].isalpha():
            cleaned = "Resource" + cleaned

        # If empty after cleaning, use a default
        if not cleaned:
            cleaned = "Resource"

        return cleaned

    def _simplify_generated_name(self, name: str) -> str:
        """Simplify CDK-generated resource names."""
        # Remove common redundant suffixes
        simplifications = [
            (r"(.*Subnet\d+)Subnet$", r"\1"),  # VpcPublicSubnet1Subnet -> VpcPublicSubnet1
            (r"(.*RouteTable\d+)RouteTable$", r"\1"),  # Similar pattern
            (r"(.*Route\d+)Route$", r"\1"),  # Similar pattern
        ]
        for pattern, replacement in simplifications:
            if re.search(pattern, name):
                name = re.sub(pattern, replacement, name)
        return name

    def _apply_semantics(self, name: str) -> str:
        patterns = [
            (re.compile(r"(.+)ServiceRole([A-F0-9]{8})?$"), r"\1Role"),
            (re.compile(r"(.+)ServiceRoleDefaultPolicy([A-F0-9]{8})?$"), r"\1Policy"),
            (re.compile(r"(.+)DefaultPolicy([A-F0-9]{8})?$"), r"\1Policy"),
            (re.compile(r"(.+)LogGroup([A-F0-9]{8})?$"), r"\1Logs"),
            (
                re.compile(r"CustomResourceProviderframework([A-F0-9]{8})?$"),
                "CustomResourceProvider",
            ),
        ]
        for rx, repl in patterns:
            if rx.match(name):
                return rx.sub(repl, name)
        return name

    def _short_hash(self, s: str) -> str:
        import hashlib

        return hashlib.md5(s.encode()).hexdigest()[:4].upper()

    def _clean_zipfile_content(self, template: dict) -> dict:
        """Clean up Lambda ZipFile content for better YAML formatting."""
        resources = template.get("Resources", {})
        if not isinstance(resources, dict):
            return template

        for resource_name, resource in resources.items():
            if not isinstance(resource, dict):
                continue

            # Check if this is a Lambda function with inline ZipFile code
            if resource.get("Type") == "AWS::Lambda::Function":
                props = resource.get("Properties", {})
                if isinstance(props, dict):
                    code = props.get("Code", {})
                    if isinstance(code, dict) and "ZipFile" in code:
                        zipfile_content = code["ZipFile"]
                        if isinstance(zipfile_content, str):
                            # Trim trailing whitespace for better block scalar formatting
                            # This allows YAML dumper to use | notation instead of quoted strings
                            code["ZipFile"] = zipfile_content.rstrip()

        return template


def _replace_sub_tokens(s: str, mapping: Dict[str, str]) -> str:
    def repl(m: re.Match[str]) -> str:
        token = m.group(1)
        # Skip pseudo-parameters and namespaces like AWS::
        if "::" in token:
            return m.group(0)
        # ${Name.Prop} → only rename the Name portion
        name = token.split(".", 1)[0]
        new = mapping.get(name, name)
        if new == name:
            return m.group(0)
        # Reconstruct with rest if present
        rest = token[len(name) :]
        return "${" + new + rest + "}"

    return SUB_TOKEN_RE.sub(repl, s)
