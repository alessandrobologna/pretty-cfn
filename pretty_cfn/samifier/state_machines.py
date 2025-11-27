"""Step Functions conversion utilities for SAM refactors."""

from __future__ import annotations

from collections import OrderedDict
from typing import Iterable, MutableMapping

from ..formatter import convert_stepfunction_definitions
from .shared import _ensure_sam_transform


_STATE_MACHINE_RENAMES = {
    "LoggingConfiguration": "Logging",
    "StateMachineName": "Name",
    "StateMachineType": "Type",
    "RoleArn": "Role",
    "DefinitionS3Location": "DefinitionUri",
    "TracingConfiguration": "Tracing",
}


def convert_state_machines(template: MutableMapping) -> bool:
    """Convert AWS::StepFunctions::StateMachine resources to Serverless equivalents."""

    resources = template.get("Resources")
    if not isinstance(resources, (dict, OrderedDict)):
        return False

    changed = False
    for logical_id, resource in list(resources.items()):
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::StepFunctions::StateMachine":
            continue
        properties = resource.get("Properties")
        if not isinstance(properties, (dict, OrderedDict)):
            continue

        if not _ensure_definition_map(logical_id, resource):
            continue

        remapped = _remap_properties(properties)
        resource["Type"] = "AWS::Serverless::StateMachine"
        resource["Properties"] = remapped
        changed = True

    if changed:
        _ensure_sam_transform(template)
    return changed


def _ensure_definition_map(logical_id: str, resource: MutableMapping) -> bool:
    properties = resource.get("Properties")
    if not isinstance(properties, (dict, OrderedDict)):
        return False
    if "DefinitionString" not in properties or "Definition" in properties:
        return True

    wrapper = {"Resources": {logical_id: resource}}
    convert_stepfunction_definitions(wrapper)
    properties = resource.get("Properties")
    if not isinstance(properties, (dict, OrderedDict)):
        return False
    return "Definition" in properties


def _remap_properties(properties: MutableMapping) -> OrderedDict:
    items: Iterable = properties.items()
    remapped: OrderedDict = OrderedDict()
    for key, value in items:
        if key == "DefinitionString":
            continue
        new_key = _STATE_MACHINE_RENAMES.get(key, key)
        remapped[new_key] = value
    return remapped
