from pretty_cfn.samifier.function_converter import samify_template
from pretty_cfn.service import TemplateProcessingError
import pytest


@pytest.fixture
def base_template():
    return {
        "Resources": {
            "Fn": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {"ZipFile": "exports.handler = () => 'hi'"},
                    "Handler": "index.handler",
                    "Runtime": "nodejs22.x",
                },
            }
        }
    }


def _create_mapping_resource(function_id, properties):
    return {
        "Type": "AWS::Lambda::EventSourceMapping",
        "Properties": {
            "FunctionName": {"Ref": function_id},
            "StartingPosition": "LATEST",
            **properties,
        },
    }


def test_self_managed_kafka_nested_configs(base_template):
    """
    Verifies SelfManagedKafka event source correctly flattens KafkaBootstrapServers
    and preserves ProvisionedPollerConfig and MetricsConfig as nested objects.
    """
    template = base_template
    template["Resources"]["Mapping"] = _create_mapping_resource(
        "Fn",
        {
            "Topics": ["Topic1"],
            "SourceAccessConfigurations": [{"Type": "SASL_SCRAM_512_AUTH", "URI": "secret"}],
            "SelfManagedEventSource": {
                "Endpoints": {"KafkaBootstrapServers": ["b-1.example.com:9092"]}
            },
            "ProvisionedPollerConfig": {"MinimumPollers": 10, "MaximumPollers": 20},
            "MetricsConfig": {"Metrics": ["EventCount"]},
        },
    )

    updated, _ = samify_template(template, asset_search_paths=[])
    fn_props = updated["Resources"]["Fn"]["Properties"]
    event_props = fn_props["Events"]["Mapping"]["Properties"]

    # Check flattened properties
    assert event_props.get("KafkaBootstrapServers") == ["b-1.example.com:9092"]

    # Check nested config preservation
    assert "ProvisionedPollerConfig" in event_props
    assert event_props["ProvisionedPollerConfig"] == {"MinimumPollers": 10, "MaximumPollers": 20}
    assert "MetricsConfig" in event_props
    assert event_props["MetricsConfig"] == {"Metrics": ["EventCount"]}

    # Check container properties removed
    assert "SelfManagedEventSource" not in event_props


def test_msk_amazon_managed_kafka_flattening_and_validation(base_template):
    """
    Verifies MSK event source correctly flattens ConsumerGroupId from
    AmazonManagedKafkaEventSourceConfig, and handles conflict/type validation.
    """
    template = base_template
    template["Resources"]["DocDb"] = {  # Add a dummy DocDb for EventSourceArn ref
        "Type": "AWS::DocDB::DBCluster",
        "Properties": {"MasterUsername": "u", "MasterUserPassword": "p"},
    }
    template["Resources"]["Mapping"] = _create_mapping_resource(
        "Fn",
        {
            "EventSourceArn": "arn:aws:kafka:us-east-1:123456789012:cluster/demo-cluster-1/6357e0b2-0e6a-4b86-a0b4-70df934c2e31-5",
            "SourceAccessConfigurations": [],  # Required for conversion to not fail
            "AmazonManagedKafkaEventSourceConfig": {"ConsumerGroupId": "my-mskcgn"},
            "Topics": ["mytopic"],  # Required for valid MSK ESM
        },
    )

    updated, _ = samify_template(template, asset_search_paths=[])
    fn_props = updated["Resources"]["Fn"]["Properties"]
    event_props = fn_props["Events"]["Mapping"]["Properties"]

    # Check flattened ConsumerGroupId
    assert event_props.get("ConsumerGroupId") == "my-mskcgn"
    # Check container removed
    assert "AmazonManagedKafkaEventSourceConfig" not in event_props


def test_msk_consumer_group_id_conflict(base_template):
    """
    Verifies that a conflict in ConsumerGroupId (top-level vs nested) raises an error.
    """
    template = base_template
    template["Resources"]["DocDb"] = {  # Add a dummy DocDb for EventSourceArn ref
        "Type": "AWS::DocDB::DBCluster",
        "Properties": {"MasterUsername": "u", "MasterUserPassword": "p"},
    }
    template["Resources"]["Mapping"] = _create_mapping_resource(
        "Fn",
        {
            "EventSourceArn": "arn:aws:kafka:us-east-1:123456789012:cluster/demo-cluster-1/6357e0b2-0e6a-4b86-a0b4-70df934c2e31-5",
            "SourceAccessConfigurations": [],
            "ConsumerGroupId": "top-level-cgn",  # Conflict
            "AmazonManagedKafkaEventSourceConfig": {"ConsumerGroupId": "nested-cgn"},
            "Topics": ["mytopic"],
        },
    )

    with pytest.raises(
        TemplateProcessingError,
        match=r"Invalid EventSourceMapping property: Event with id \[ConsumerGroupId\] is invalid\. Conflict: ConsumerGroupId specified both directly and via AmazonManagedKafkaEventSourceConfig\.",
    ):
        samify_template(template, asset_search_paths=[])


def test_msk_consumer_group_id_type_validation(base_template):
    """
    Verifies that incorrect type for ConsumerGroupId in AmazonManagedKafkaEventSourceConfig raises an error.
    """
    template = base_template
    template["Resources"]["DocDb"] = {  # Add a dummy DocDb for EventSourceArn ref
        "Type": "AWS::DocDB::DBCluster",
        "Properties": {"MasterUsername": "u", "MasterUserPassword": "p"},
    }
    template["Resources"]["Mapping"] = _create_mapping_resource(
        "Fn",
        {
            "EventSourceArn": "arn:aws:kafka:us-east-1:123456789012:cluster/demo-cluster-1/6357e0b2-0e6a-4b86-a0b4-70df934c2e31-5",
            "SourceAccessConfigurations": [],
            "AmazonManagedKafkaEventSourceConfig": {
                "ConsumerGroupId": 123  # Invalid type
            },
            "Topics": ["mytopic"],
        },
    )

    with pytest.raises(
        TemplateProcessingError,
        match="ConsumerGroupId from AmazonManagedKafkaEventSourceConfig must be a string or intrinsic function.",
    ):
        samify_template(template, asset_search_paths=[])
