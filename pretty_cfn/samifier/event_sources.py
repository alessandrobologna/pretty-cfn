"""Helpers for folding event-source style integrations into SAM events."""

from __future__ import annotations

from collections import OrderedDict
from typing import Optional

from ..exceptions import TemplateProcessingError
from ..formatter import CFNTag
from .shared import _extract_logical_id, _remove_resources


class InvalidEventException(Exception):
    def __init__(self, property_name: str, message: str, *args, **kwargs):
        full_message = f"Event with id [{property_name}] is invalid. {message}"
        super().__init__(full_message, *args, **kwargs)
        self.property_name = property_name
        self.message = full_message


def fold_event_source_mappings(template, converted_functions) -> bool:
    resources = template.get("Resources")
    if not isinstance(resources, (dict, OrderedDict)):
        return False

    mappings_to_remove: list[str] = []
    changed = False

    for logical_id, resource in list(resources.items()):
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::Lambda::EventSourceMapping":
            continue
        properties = resource.get("Properties")
        if not isinstance(properties, (dict, OrderedDict)):
            continue
        properties = OrderedDict(properties)
        function_id = _extract_logical_id(properties.get("FunctionName"))
        if not function_id or function_id not in converted_functions:
            continue

        event_def = _convert_event_source_mapping(properties, resources)
        if event_def is None:
            continue

        function_res = converted_functions[function_id]
        fn_props = function_res.setdefault("Properties", OrderedDict())
        events_block = fn_props.setdefault("Events", OrderedDict())
        event_name = logical_id
        suffix = 1
        while event_name in events_block:
            event_name = f"{logical_id}{suffix}"
            suffix += 1
        events_block[event_name] = event_def
        mappings_to_remove.append(logical_id)
        changed = True

    _remove_resources(template, mappings_to_remove)
    return changed


def fold_push_events(template, converted_functions) -> bool:
    resources = template.get("Resources")
    if not isinstance(resources, (dict, OrderedDict)):
        return False

    rule_ids: list[str] = []
    perm_ids: list[str] = []
    bucket_updates: list[tuple[str, list[int]]] = []  # (bucket_id, lambda_config_indexes_to_remove)
    changed = False

    # EventBridge / Schedule rules
    for logical_id, resource in list(resources.items()):
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::Events::Rule":
            continue
        props = resource.get("Properties")
        if not isinstance(props, (dict, OrderedDict)):
            continue
        props = OrderedDict(props)
        targets = props.get("Targets")
        if not isinstance(targets, list) or len(targets) != 1:
            continue
        target = targets[0] if targets else None
        if not isinstance(target, (dict, OrderedDict)):
            continue
        target = OrderedDict(target)
        fn_id = _extract_logical_id(target.get("Arn"))
        if not fn_id or fn_id not in converted_functions:
            continue
        if "InputTransformer" in target:
            continue  # unsupported; skip

        event_def = _convert_events_rule(props, target)
        if event_def is None:
            continue

        function_res = converted_functions[fn_id]
        fn_props = function_res.setdefault("Properties", OrderedDict())
        events_block = fn_props.setdefault("Events", OrderedDict())
        event_name = logical_id
        suffix = 1
        while event_name in events_block:
            event_name = f"{logical_id}{suffix}"
            suffix += 1
        events_block[event_name] = event_def
        rule_ids.append(logical_id)
        # Remove paired permission if it targets same function/rule
        perm_ids.extend(_find_lambda_permissions_for_rule(resources, logical_id, fn_id))
        changed = True

    # S3 bucket notifications
    for bucket_id, resource in list(resources.items()):
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::S3::Bucket":
            continue
        props = resource.get("Properties") or {}
        notif = props.get("NotificationConfiguration")
        if not isinstance(notif, (dict, OrderedDict)):
            continue
        lambdas = notif.get("LambdaConfigurations")
        if not isinstance(lambdas, list):
            continue
        remove_indexes: list[int] = []
        lambda_count = len(lambdas)
        for idx, cfg in enumerate(lambdas):
            if not isinstance(cfg, (dict, OrderedDict)):
                continue
            cfg = OrderedDict(cfg)
            fn_id = _extract_logical_id(cfg.get("Function"))
            if not fn_id or fn_id not in converted_functions:
                continue
            event_def = _convert_s3_notification(bucket_id, cfg)
            if event_def is None:
                continue
            function_res = converted_functions[fn_id]
            fn_props = function_res.setdefault("Properties", OrderedDict())
            events_block = fn_props.setdefault("Events", OrderedDict())
            # Use a stable, human‑readable name. When there is only a single
            # Lambda notification for this bucket we keep the bare bucket id;
            # when multiple notifications exist we suffix them with an index
            # for uniqueness.
            if lambda_count == 1:
                base_name = bucket_id
            else:
                base_name = f"{bucket_id}{idx}"
            event_name = base_name
            suffix = 1
            while event_name in events_block:
                event_name = f"{base_name}{suffix}"
                suffix += 1
            events_block[event_name] = event_def
            remove_indexes.append(idx)
            perm_ids.extend(_find_s3_permissions(resources, fn_id, bucket_id))
            changed = True
        if remove_indexes:
            bucket_updates.append((bucket_id, remove_indexes))

    # Apply removals
    for bucket_id, idx_list in bucket_updates:
        bucket_res = resources.get(bucket_id)
        if not isinstance(bucket_res, (dict, OrderedDict)):
            continue
        props = bucket_res.get("Properties") or {}
        lambdas = props.get("NotificationConfiguration", {}).get("LambdaConfigurations")
        if isinstance(lambdas, list):
            for idx in sorted(idx_list, reverse=True):
                if 0 <= idx < len(lambdas):
                    lambdas.pop(idx)
            if not lambdas:
                props.get("NotificationConfiguration", {}).pop("LambdaConfigurations", None)
            if not props.get("NotificationConfiguration"):
                props.pop("NotificationConfiguration", None)

    _remove_resources(template, rule_ids)
    _remove_resources(template, perm_ids)
    return changed


def fold_iot_rules(template, converted_functions) -> bool:
    resources = template.get("Resources")
    if not isinstance(resources, (dict, OrderedDict)):
        return False

    changed = False
    rules_to_remove: list[str] = []
    perms_to_remove: list[str] = []

    for logical_id, resource in list(resources.items()):
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::IoT::TopicRule":
            continue
        props = resource.get("Properties") or {}
        actions = props.get("Actions")
        if not isinstance(actions, list) or len(actions) != 1:
            continue
        action = actions[0]
        if not isinstance(action, (dict, OrderedDict)) or "Lambda" not in action:
            continue
        lambda_block = action.get("Lambda") or {}
        fn_id = _extract_logical_id(lambda_block.get("FunctionArn"))
        if not fn_id or fn_id not in converted_functions:
            continue

        event = OrderedDict()
        event["Type"] = "IoTRule"
        ev_props = OrderedDict()
        if props.get("TopicRulePayload") is not None:
            ev_props["Sql"] = (
                props["TopicRulePayload"].get("Sql")
                if isinstance(props["TopicRulePayload"], (dict, OrderedDict))
                else None
            )
            ev_props["Description"] = (
                props["TopicRulePayload"].get("Description")
                if isinstance(props["TopicRulePayload"], (dict, OrderedDict))
                else None
            )
            ev_props["RuleDisabled"] = (
                props["TopicRulePayload"].get("RuleDisabled")
                if isinstance(props["TopicRulePayload"], (dict, OrderedDict))
                else None
            )
            if props["TopicRulePayload"].get("AwsIotSqlVersion") is not None:
                ev_props["AwsIotSqlVersion"] = props["TopicRulePayload"].get("AwsIotSqlVersion")
        if not ev_props.get("Sql"):
            continue
        event["Properties"] = ev_props

        function_res = converted_functions[fn_id]
        fn_props = function_res.setdefault("Properties", OrderedDict())
        events_block = fn_props.setdefault("Events", OrderedDict())
        event_name = logical_id
        suffix = 1
        while event_name in events_block:
            event_name = f"{logical_id}{suffix}"
            suffix += 1
        events_block[event_name] = event
        rules_to_remove.append(logical_id)
        perms_to_remove.extend(_find_lambda_permissions_for_iot(resources, fn_id))
        changed = True

    _remove_resources(template, rules_to_remove)
    _remove_resources(template, perms_to_remove)
    return changed


def _find_lambda_permissions_for_iot(resources: dict, function_id: str) -> list[str]:
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
        principal = props.get("Principal")
        if isinstance(principal, str) and principal == "iot.amazonaws.com":
            perm_ids.append(logical_id)
    return perm_ids


def fold_cognito_triggers(template, converted_functions) -> bool:
    resources = template.get("Resources")
    if not isinstance(resources, (dict, OrderedDict)):
        return False

    changed = False

    for logical_id, resource in list(resources.items()):
        if not isinstance(resource, (dict, OrderedDict)):
            continue
        if resource.get("Type") != "AWS::Cognito::UserPool":
            continue
        props = resource.get("Properties") or {}
        triggers = props.get("LambdaConfig")
        if not isinstance(triggers, (dict, OrderedDict)):
            continue

        for trigger_name, ref in list(triggers.items()):
            fn_id = _extract_logical_id(ref)
            if not fn_id or fn_id not in converted_functions:
                continue
            function_res = converted_functions[fn_id]
            fn_props = function_res.setdefault("Properties", OrderedDict())
            events_block = fn_props.setdefault("Events", OrderedDict())
            event_key = f"{logical_id}{trigger_name}"
            event = OrderedDict(
                [
                    ("Type", "Cognito"),
                    (
                        "Properties",
                        OrderedDict(
                            [
                                ("UserPool", {"Ref": logical_id}),
                                ("Trigger", trigger_name),
                            ]
                        ),
                    ),
                ]
            )
            events_block[event_key] = event
            triggers.pop(trigger_name, None)
            changed = True

        if not triggers:
            props.pop("LambdaConfig", None)

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


def _convert_s3_notification(bucket_id: str, cfg: OrderedDict) -> Optional[OrderedDict]:
    events = cfg.get("Event") or cfg.get("Events")
    if not events:
        return None
    # normalize to list
    event_list = events if isinstance(events, list) else [events]
    props = OrderedDict()
    props["Bucket"] = {"Ref": bucket_id}
    props["Events"] = event_list
    filter_rules = _extract_s3_filter_rules(cfg.get("Filter"))
    if filter_rules:
        props["Filter"] = {"S3Key": {"Rules": filter_rules}}
    return OrderedDict([("Type", "S3"), ("Properties", props)])


def _extract_s3_filter_rules(filter_block) -> Optional[list]:
    if not isinstance(filter_block, (dict, OrderedDict)):
        return None
    s3key = filter_block.get("S3Key") if isinstance(filter_block, (dict, OrderedDict)) else None
    rules = s3key.get("Rules") if isinstance(s3key, (dict, OrderedDict)) else None
    if not isinstance(rules, list):
        return None
    normalized = []
    for rule in rules:
        if not isinstance(rule, (dict, OrderedDict)):
            continue
        name = rule.get("Name")
        value = rule.get("Value")
        if name in {"prefix", "suffix"} and value is not None:
            normalized.append({"Name": name, "Value": value})
    return normalized or None


def _find_s3_permissions(resources: dict, function_id: str, bucket_id: str) -> list[str]:
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
        principal = props.get("Principal")
        if isinstance(principal, str) and principal != "s3.amazonaws.com":
            continue
        source_arn = props.get("SourceArn")
        if source_arn and bucket_id in repr(source_arn):
            perm_ids.append(logical_id)
    return perm_ids


_COMMON_MAPPING_KEYS = {
    "BatchSize",
    "Enabled",
    "StartingPosition",
    "StartingPositionTimestamp",
    "MaximumBatchingWindowInSeconds",
    "MaximumRetryAttempts",
    "BisectBatchOnFunctionError",
    "MaximumRecordAgeInSeconds",
    "ParallelizationFactor",
    "DestinationConfig",
    "FunctionResponseTypes",
    "FilterCriteria",
    "TumblingWindowInSeconds",
    "ScalingConfig",
    "ConsumerGroupId",
    "ProvisionedPollerConfig",
    "MetricsConfig",
}

_INPUT_ONLY_KEYS = {
    "DocumentDBEventSourceConfig",
    "SelfManagedEventSource",
    "AmazonManagedKafkaEventSourceConfig",
}


def _convert_event_source_mapping(
    properties: OrderedDict, resources: dict
) -> Optional[OrderedDict]:
    try:
        detected = _detect_event_source_type(properties, resources)
        if detected is None:
            return None
        event_type, sam_target_key, source_target_key, extra_allowed = detected
        if event_type is None:
            return None

        allowed_keys = set(_COMMON_MAPPING_KEYS)
        allowed_keys.update(extra_allowed)
        allowed_keys.add(source_target_key)

        for key in properties.keys():
            if key not in allowed_keys:
                if key == "FunctionName":
                    continue
                return None

        event_props = OrderedDict()

        target_value = properties.get(source_target_key)
        if target_value is None and source_target_key == "KafkaBootstrapServers":
            smes = properties.get("SelfManagedEventSource")
            if isinstance(smes, (dict, OrderedDict)):
                endpoints = smes.get("Endpoints")
                if isinstance(endpoints, (dict, OrderedDict)):
                    target_value = endpoints.get("KafkaBootstrapServers")
        if target_value is None:
            return None
        event_props[sam_target_key] = target_value

        # Flatten AmazonManagedKafkaEventSourceConfig
        amk_config = properties.get("AmazonManagedKafkaEventSourceConfig")
        if isinstance(amk_config, (dict, OrderedDict)):
            if "ConsumerGroupId" in amk_config:
                # Conflict detection: If ConsumerGroupId specified both directly (top-level) and nested
                if (
                    "ConsumerGroupId" in properties
                ):  # Check if top-level ConsumerGroupId exists in input
                    raise InvalidEventException(
                        "ConsumerGroupId",
                        "Conflict: ConsumerGroupId specified both directly and via AmazonManagedKafkaEventSourceConfig.",
                    )
                consumer_group_id = amk_config["ConsumerGroupId"]
                if not isinstance(consumer_group_id, (str, dict, OrderedDict)):
                    raise InvalidEventException(
                        "ConsumerGroupId",
                        "ConsumerGroupId from AmazonManagedKafkaEventSourceConfig must be a string or intrinsic function.",
                    )
                event_props["ConsumerGroupId"] = consumer_group_id

        for key in _COMMON_MAPPING_KEYS:
            if key in properties:
                event_props[key] = properties[key]

        for key in extra_allowed:
            if key in properties and key != source_target_key:
                if key in _INPUT_ONLY_KEYS:
                    continue
                event_props[key] = properties[key]

        if event_type == "DocumentDB":
            cfg = properties.get("DocumentDBEventSourceConfig")
            if isinstance(cfg, (dict, OrderedDict)):
                if "DatabaseName" in cfg:
                    event_props["DatabaseName"] = cfg["DatabaseName"]
                if "CollectionName" in cfg:
                    event_props["CollectionName"] = cfg["CollectionName"]
                if "FullDocument" in cfg:
                    event_props["FullDocument"] = cfg["FullDocument"]
            if "DatabaseName" not in event_props:
                return None
            if (
                "SourceAccessConfigurations" not in event_props
                and "SourceAccessConfigurations" in properties
            ):
                event_props["SourceAccessConfigurations"] = properties.get(
                    "SourceAccessConfigurations"
                )
            if "SourceAccessConfigurations" not in event_props:
                return None
            if "StartingPosition" not in event_props:
                return None

        return OrderedDict([("Type", event_type), ("Properties", event_props)])
    except InvalidEventException as e:
        raise TemplateProcessingError(f"Invalid EventSourceMapping property: {e.message}") from e


def _detect_event_source_type(
    properties: OrderedDict, resources: dict
) -> Optional[tuple[str, str, str, set[str]]]:
    # Self-managed Kafka uses SelfManagedEventSource
    if "SelfManagedEventSource" in properties:
        extra = {
            "Topics",
            "ConsumerGroupId",
            "SourceAccessConfigurations",
            "SchemaRegistryConfig",
            "SelfManagedEventSource",
            "ProvisionedPollerConfig",
            "MetricsConfig",
        }
        # Normalize bootstrap servers field if present
        smes = properties.get("SelfManagedEventSource")
        if isinstance(smes, (dict, OrderedDict)):
            endpoints = smes.get("Endpoints")
            if isinstance(endpoints, (dict, OrderedDict)) and "KafkaBootstrapServers" in endpoints:
                properties = OrderedDict(properties)
                properties["KafkaBootstrapServers"] = endpoints.get("KafkaBootstrapServers")
                extra.add("KafkaBootstrapServers")
        return "SelfManagedKafka", "KafkaBootstrapServers", "KafkaBootstrapServers", extra

    event_source_arn = properties.get("EventSourceArn")
    if not event_source_arn:
        return None

    # Try to infer type from referenced resource, if intrinsic
    referenced_type = _resource_type_from_reference(event_source_arn, resources)
    if referenced_type:
        if referenced_type == "AWS::SQS::Queue":
            return "SQS", "Queue", "EventSourceArn", set()
        if referenced_type == "AWS::Kinesis::Stream":
            return "Kinesis", "Stream", "EventSourceArn", set()
        if referenced_type == "AWS::DynamoDB::Table":
            return "DynamoDB", "Stream", "EventSourceArn", set()
        if referenced_type == "AWS::MSK::Cluster":
            extra = {
                "Topics",
                "ConsumerGroupId",
                "SourceAccessConfigurations",
                "SchemaRegistryConfig",
                "AmazonManagedKafkaEventSourceConfig",
                "ProvisionedPollerConfig",
                "MetricsConfig",
            }
            return "MSK", "Stream", "EventSourceArn", extra
        if referenced_type == "AWS::AmazonMQ::Broker":
            extra = {"Queues", "SourceAccessConfigurations"}
            return "MQ", "Broker", "EventSourceArn", extra
        if referenced_type == "AWS::DocDB::DBCluster":
            extra = {
                "DocumentDBEventSourceConfig",
                "SourceAccessConfigurations",
                "SecretsManagerKmsKeyId",
            }
            return "DocumentDB", "Cluster", "EventSourceArn", extra

    arn_repr = repr(event_source_arn).lower()

    # MSK
    if "kafka" in arn_repr and "cluster" in arn_repr:
        extra = {
            "Topics",
            "ConsumerGroupId",
            "SourceAccessConfigurations",
            "SchemaRegistryConfig",
            "AmazonManagedKafkaEventSourceConfig",
            "ProvisionedPollerConfig",
            "MetricsConfig",
        }
        return "MSK", "Stream", "EventSourceArn", extra

    # MQ
    if ":mq:" in arn_repr:
        extra = {"Queues", "SourceAccessConfigurations"}
        return "MQ", "Broker", "EventSourceArn", extra

    # DocumentDB
    if (
        ":docdb:" in arn_repr
        or ":rds:" in arn_repr
        and ":cluster:" in arn_repr
        and "docdb" in arn_repr
    ):
        extra = {
            "DocumentDBEventSourceConfig",
            "SourceAccessConfigurations",
            "SecretsManagerKmsKeyId",
        }
        return "DocumentDB", "Cluster", "EventSourceArn", extra

    # DynamoDB
    if ":dynamodb:" in arn_repr:
        return "DynamoDB", "Stream", "EventSourceArn", set()

    # Kinesis
    if ":kinesis:" in arn_repr:
        return "Kinesis", "Stream", "EventSourceArn", set()

    # SQS
    if ":sqs:" in arn_repr:
        return "SQS", "Queue", "EventSourceArn", set()

    return None


def _resource_type_from_reference(ref_value, resources: dict) -> Optional[str]:
    logical_id = None
    if isinstance(ref_value, (dict, OrderedDict)):
        if "Ref" in ref_value and isinstance(ref_value["Ref"], str):
            logical_id = ref_value["Ref"]
        elif "Fn::GetAtt" in ref_value:
            target = ref_value["Fn::GetAtt"]
            if isinstance(target, list) and target:
                logical_id = target[0]
            elif isinstance(target, str):
                logical_id = target.split(".", 1)[0]
    elif isinstance(ref_value, CFNTag):
        logical_id = _extract_logical_id(ref_value)
    if not logical_id:
        return None
    resource = resources.get(logical_id)
    if not isinstance(resource, (dict, OrderedDict)):
        return None
    return resource.get("Type")
