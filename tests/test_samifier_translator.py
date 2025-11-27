import copy
import json
from pathlib import Path

import pytest

from pretty_cfn.samifier.function_converter import samify_template


class _StaticManagedPolicyLoader:
    """Stub loader to avoid IAM calls during translator runs."""

    def __init__(self, policy_map=None):
        self._policy_map = policy_map or {}

    def load(self):
        return self._policy_map


@pytest.mark.parametrize(
    "template",
    [
        {
            "Resources": {
                "MyQueue": {"Type": "AWS::SQS::Queue"},
                "MyFunc": {
                    "Type": "AWS::Lambda::Function",
                    "Properties": {
                        "Handler": "index.handler",
                        "Runtime": "python3.11",
                        "Code": {"ZipFile": "def handler(event, ctx): return {'statusCode': 200}"},
                        "Timeout": 30,
                    },
                },
                "QueueMapping": {
                    "Type": "AWS::Lambda::EventSourceMapping",
                    "Properties": {
                        "EventSourceArn": {"Fn::GetAtt": ["MyQueue", "Arn"]},
                        "FunctionName": {"Ref": "MyFunc"},
                        "BatchSize": 5,
                        "StartingPosition": "LATEST",
                    },
                },
                "ScheduleRule": {
                    "Type": "AWS::Events::Rule",
                    "Properties": {
                        "ScheduleExpression": "rate(5 minutes)",
                        "State": "ENABLED",
                        "Targets": [
                            {
                                "Id": "Target0",
                                "Arn": {"Fn::GetAtt": ["MyFunc", "Arn"]},
                            }
                        ],
                    },
                },
            }
        }
    ],
    ids=["sqs_and_schedule"],
)
def test_samified_templates_transform_with_sam_translator(template, tmp_path: Path):
    pytest.importorskip("samtranslator")
    from samtranslator.translator.transform import transform

    working = copy.deepcopy(template)
    updated, changed = samify_template(working, asset_search_paths=[], relative_to=tmp_path)

    assert changed
    assert updated.get("Transform") == "AWS::Serverless-2016-10-31"

    loader = _StaticManagedPolicyLoader()
    result = transform(
        input_fragment=json.loads(json.dumps(updated)),  # ensure JSON-serializable
        parameter_values={},
        managed_policy_loader=loader,
    )

    assert result.get("Resources")
