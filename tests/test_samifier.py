import json
import shutil
import yaml
from pathlib import Path
from typing import Optional
import zipfile

import io

from pretty_cfn.formatter import CFNTag, LiteralStr, create_cfn_yaml
from pretty_cfn.samifier import (
    AwsEnvironment,
    SamAssetStager,
    convert_appsync_apis,
    convert_state_machines,
    samify_template,
    rewrite_function_url_refs,
    _prepare_inline_code,
)
from pretty_cfn.service import TemplateProcessingOptions, TemplateSource, process_template
from pretty_cfn.samifier.optimizations import (
    apply_function_globals,
    strip_cdk_metadata,
)


def test_prepare_inline_code_expands_tabs():
    literal = _prepare_inline_code("line1\\n\tline2\n")
    assert isinstance(literal, LiteralStr)
    assert "\t" not in literal
    assert "line2" in literal


def test_samify_converts_lambda_asset(tmp_path):
    template = json.loads(
        json.dumps(
            {
                "Resources": {
                    "MyFunc": {
                        "Type": "AWS::Lambda::Function",
                        "Properties": {
                            "Code": {
                                "S3Bucket": {"Ref": "AssetBucket"},
                                "S3Key": "asset.zip",
                            },
                            "Handler": "index.handler",
                            "Runtime": "nodejs22.x",
                        },
                        "Metadata": {
                            "aws:cdk:path": "Stack/MyFunc/Resource",
                            "aws:asset:path": "asset.123",
                            "aws:asset:property": "Code",
                        },
                    }
                }
            }
        )
    )

    asset_dir = tmp_path / "cdk.out"
    asset_path = asset_dir / "asset.123"
    asset_path.mkdir(parents=True)

    updated, changed = samify_template(
        template,
        asset_search_paths=[asset_dir],
        relative_to=tmp_path,
    )

    assert changed
    func = updated["Resources"]["MyFunc"]
    assert func["Type"] == "AWS::Serverless::Function"
    code_uri = func["Properties"]["CodeUri"]
    assert code_uri == f"cdk.out/{asset_path.name}"
    assert updated["Transform"] == "AWS::Serverless-2016-10-31"


def test_samify_folds_function_url(tmp_path):
    template = {
        "Resources": {
            "MyFunc": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {
                        "S3Bucket": {"Ref": "Bucket"},
                        "S3Key": "asset.zip",
                    },
                    "Handler": "index.handler",
                    "Runtime": "nodejs22.x",
                },
                "Metadata": {
                    "aws:asset:path": "asset.123",
                    "aws:asset:property": "Code",
                },
            },
            "LegacyUrlResource": {
                "Type": "AWS::Lambda::Url",
                "Properties": {
                    "AuthType": "NONE",
                    "Cors": {"AllowOrigins": ["*"]},
                    "TargetFunctionArn": {"Fn::GetAtt": ["MyFunc", "Arn"]},
                },
            },
            "InvokePerm": {
                "Type": "AWS::Lambda::Permission",
                "Properties": {
                    "Action": "lambda:InvokeFunctionUrl",
                    "FunctionName": {"Fn::GetAtt": ["MyFunc", "Arn"]},
                    "FunctionUrlAuthType": "NONE",
                    "Principal": "*",
                },
            },
        }
    }

    template["Outputs"] = {
        "FunctionUrl": {"Value": {"Fn::GetAtt": ["LegacyUrlResource", "FunctionUrl"]}}
    }

    asset_dir = tmp_path / "cdk.out"
    asset_path = asset_dir / "asset.123"
    asset_path.mkdir(parents=True)

    updated, changed = samify_template(
        template,
        asset_search_paths=[asset_dir],
        relative_to=tmp_path,
    )

    assert changed
    resources = updated["Resources"]
    assert "LegacyUrlResource" not in resources
    assert "InvokePerm" not in resources
    func = resources["MyFunc"]
    assert func["Type"] == "AWS::Serverless::Function"
    assert func["Properties"]["FunctionUrlConfig"]["AuthType"] == "NONE"
    output = updated["Outputs"]["FunctionUrl"]["Value"]
    assert output["Fn::GetAtt"][0] == "MyFuncUrl"
    assert updated["Transform"] == "AWS::Serverless-2016-10-31"


def test_samify_preserves_existing_transform(tmp_path):
    template = {
        "Transform": ["AWS::Include"],
        "Resources": {
            "Func": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {"S3Bucket": "Bucket", "S3Key": "key"},
                    "Handler": "index.handler",
                    "Runtime": "nodejs22.x",
                },
                "Metadata": {
                    "aws:asset:path": "asset.abc",
                },
            }
        },
    }

    asset_dir = tmp_path / "cdk.out"
    asset_dir.mkdir()

    updated, changed = samify_template(
        template,
        asset_search_paths=[asset_dir],
        relative_to=tmp_path,
    )

    assert changed
    assert "AWS::Serverless-2016-10-31" in updated["Transform"]


def test_event_source_mapping_sqs(tmp_path):
    template = {
        "Resources": {
            "Worker": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {"S3Bucket": "b", "S3Key": "k"},
                    "Handler": "app.handler",
                    "Runtime": "python3.12",
                },
            },
            "Queue": {"Type": "AWS::SQS::Queue"},
            "WorkerQueueMapping": {
                "Type": "AWS::Lambda::EventSourceMapping",
                "Properties": {
                    "EventSourceArn": {"Fn::GetAtt": ["Queue", "Arn"]},
                    "FunctionName": {"Ref": "Worker"},
                    "BatchSize": 10,
                    "Enabled": True,
                },
            },
        }
    }

    updated, changed = samify_template(template, asset_search_paths=[], relative_to=tmp_path)

    assert changed
    resources = updated["Resources"]
    fn = resources["Worker"]
    events = fn["Properties"].get("Events")
    assert events
    mapping_event = events.get("WorkerQueueMapping")
    assert mapping_event["Type"] == "SQS"
    props = mapping_event["Properties"]
    assert props["Queue"] == {"Fn::GetAtt": ["Queue", "Arn"]}
    assert props["BatchSize"] == 10
    assert "WorkerQueueMapping" not in resources


def test_event_source_mapping_skips_unknown_property(tmp_path):
    template = {
        "Resources": {
            "Fn": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {"ZipFile": "exports.handler = () => 'hi'"},
                    "Handler": "index.handler",
                    "Runtime": "nodejs22.x",
                },
            },
            "Map": {
                "Type": "AWS::Lambda::EventSourceMapping",
                "Properties": {
                    "EventSourceArn": "arn:aws:sqs:::q",
                    "FunctionName": {"Ref": "Fn"},
                    "BatchSize": 5,
                    "Unsupported": True,
                },
            },
        }
    }

    updated, changed = samify_template(template, asset_search_paths=[], relative_to=tmp_path)

    resources = updated["Resources"]
    assert "Map" in resources  # skipped due to unsupported property
    fn = resources["Fn"]
    assert fn["Type"] == "AWS::Serverless::Function"
    events = fn["Properties"].get("Events") or {}
    assert "Map" not in events


def test_schedule_rule_converts_to_schedule_event(tmp_path):
    template = {
        "Resources": {
            "Fn": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {"ZipFile": "exports.handler = () => 'hi'"},
                    "Handler": "index.handler",
                    "Runtime": "nodejs22.x",
                },
            },
            "Rule": {
                "Type": "AWS::Events::Rule",
                "Properties": {
                    "ScheduleExpression": "rate(1 minute)",
                    "State": "ENABLED",
                    "Targets": [
                        {
                            "Id": "Target1",
                            "Arn": {"Fn::GetAtt": ["Fn", "Arn"]},
                            "Input": {"foo": "bar"},
                        }
                    ],
                },
            },
            "InvokePerm": {
                "Type": "AWS::Lambda::Permission",
                "Properties": {
                    "FunctionName": {"Ref": "Fn"},
                    "Action": "lambda:InvokeFunction",
                    "Principal": "events.amazonaws.com",
                    "SourceArn": {"Ref": "Rule"},
                },
            },
        }
    }

    updated, changed = samify_template(template, asset_search_paths=[], relative_to=tmp_path)

    assert changed
    fn = updated["Resources"]["Fn"]
    events = fn["Properties"].get("Events")
    assert events and "Rule" in events
    event = events["Rule"]
    assert event["Type"] == "Schedule"
    props = event["Properties"]
    assert props["Schedule"] == "rate(1 minute)"
    assert props["Enabled"] is True
    assert props["Input"] == {"foo": "bar"}
    resources = updated["Resources"]
    assert "Rule" not in resources
    assert "InvokePerm" not in resources


def test_schedule_rule_with_input_transformer_skips(tmp_path):
    template = {
        "Resources": {
            "Fn": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {"ZipFile": "exports.handler = () => 'hi'"},
                    "Handler": "index.handler",
                    "Runtime": "nodejs22.x",
                },
            },
            "Rule": {
                "Type": "AWS::Events::Rule",
                "Properties": {
                    "ScheduleExpression": "rate(5 minutes)",
                    "Targets": [
                        {
                            "Id": "Target1",
                            "Arn": {"Fn::GetAtt": ["Fn", "Arn"]},
                            "InputTransformer": {"InputTemplate": "<foo>"},
                        }
                    ],
                },
            },
        }
    }

    updated, _ = samify_template(template, asset_search_paths=[], relative_to=tmp_path)

    resources = updated["Resources"]
    assert "Rule" in resources  # skipped due to InputTransformer
    fn = resources["Fn"]
    events = fn["Properties"].get("Events") or {}
    assert "Rule" not in events


def test_s3_notification_converts(tmp_path):
    template = {
        "Resources": {
            "Fn": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {"ZipFile": "exports.handler = () => 'hi'"},
                    "Handler": "index.handler",
                    "Runtime": "nodejs22.x",
                },
            },
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {
                    "NotificationConfiguration": {
                        "LambdaConfigurations": [
                            {
                                "Event": "s3:ObjectCreated:*",
                                "Function": {"Fn::GetAtt": ["Fn", "Arn"]},
                                "Filter": {
                                    "S3Key": {
                                        "Rules": [
                                            {"Name": "prefix", "Value": "images/"},
                                            {"Name": "suffix", "Value": ".jpg"},
                                        ]
                                    }
                                },
                            }
                        ]
                    }
                },
            },
            "InvokePerm": {
                "Type": "AWS::Lambda::Permission",
                "Properties": {
                    "FunctionName": {"Ref": "Fn"},
                    "Principal": "s3.amazonaws.com",
                    "SourceArn": {"Fn::Sub": "arn:aws:s3:::${Bucket}"},
                },
            },
        }
    }

    updated, changed = samify_template(template, asset_search_paths=[], relative_to=tmp_path)

    assert changed
    resources = updated["Resources"]
    fn = resources["Fn"]
    events = fn["Properties"].get("Events")
    assert events
    ev = next(iter(events.values()))
    assert ev["Type"] == "S3"
    props = ev["Properties"]
    assert props["Bucket"] == {"Ref": "Bucket"}
    assert props["Events"] == ["s3:ObjectCreated:*"]
    assert props["Filter"] == {
        "S3Key": {
            "Rules": [{"Name": "prefix", "Value": "images/"}, {"Name": "suffix", "Value": ".jpg"}]
        }
    }
    bucket = resources["Bucket"]
    notif = bucket.get("Properties", {}).get("NotificationConfiguration")
    assert not notif or not notif.get("LambdaConfigurations")
    assert "InvokePerm" not in resources


def test_documentdb_event_source_mapping_converts(tmp_path):
    template = {
        "Resources": {
            "Fn": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {"ZipFile": "exports.handler = () => 'hi'"},
                    "Handler": "index.handler",
                    "Runtime": "nodejs22.x",
                },
            },
            "DocDb": {
                "Type": "AWS::DocDB::DBCluster",
                "Properties": {"MasterUsername": "u", "MasterUserPassword": "p"},
            },
            "Mapping": {
                "Type": "AWS::Lambda::EventSourceMapping",
                "Properties": {
                    "EventSourceArn": {"Fn::GetAtt": ["DocDb", "Arn"]},
                    "FunctionName": {"Ref": "Fn"},
                    "StartingPosition": "LATEST",
                    "SourceAccessConfigurations": [
                        {
                            "Type": "BASIC_AUTH",
                            "URI": "arn:aws:secretsmanager:us-east-1:123456789012:secret:creds",
                        }
                    ],
                    "DocumentDBEventSourceConfig": {
                        "DatabaseName": "appdb",
                        "CollectionName": "items",
                        "FullDocument": "UpdateLookup",
                    },
                },
            },
        }
    }

    updated, changed = samify_template(template, asset_search_paths=[], relative_to=tmp_path)

    assert changed
    resources = updated["Resources"]
    assert "Mapping" not in resources  # folded
    fn = resources["Fn"]
    events = fn["Properties"].get("Events") or {}
    assert "Mapping" in events
    mapping = events["Mapping"]
    assert mapping["Type"] == "DocumentDB"
    props = mapping["Properties"]
    assert props["Cluster"] == {"Fn::GetAtt": ["DocDb", "Arn"]}
    assert props["DatabaseName"] == "appdb"
    assert props.get("CollectionName") == "items"
    assert props.get("FullDocument") == "UpdateLookup"
    assert props.get("StartingPosition") == "LATEST"
    assert "DocumentDBEventSourceConfig" not in props
    assert props.get("SourceAccessConfigurations") == [
        {"Type": "BASIC_AUTH", "URI": "arn:aws:secretsmanager:us-east-1:123456789012:secret:creds"}
    ]


def test_rest_api_shell_converts_when_orphaned(tmp_path):
    template = {
        "Resources": {
            "Rest": {
                "Type": "AWS::ApiGateway::RestApi",
                "Properties": {"Name": "api", "Description": "desc"},
            },
            "Deployment": {
                "Type": "AWS::ApiGateway::Deployment",
                "Properties": {"RestApiId": {"Ref": "Rest"}},
            },
            "Stage": {
                "Type": "AWS::ApiGateway::Stage",
                "Properties": {"RestApiId": {"Ref": "Rest"}, "StageName": "prod"},
            },
        }
    }

    updated, changed = samify_template(template, asset_search_paths=[], relative_to=tmp_path)

    # structure_changed may be false if only type swap; assert resource transformed
    resources = updated["Resources"]
    rest = resources["Rest"]
    assert rest["Type"] == "AWS::Serverless::Api"
    assert rest["Properties"]["Name"] == "api"
    assert rest["Properties"]["Description"] == "desc"
    assert "Deployment" not in resources
    assert "Stage" not in resources


def test_rest_api_shell_with_cors_and_stage_folds_cors_and_stage(tmp_path):
    template = {
        "Resources": {
            "Rest": {
                "Type": "AWS::ApiGateway::RestApi",
                "Properties": {"Name": "api"},
            },
            "Deployment": {
                "Type": "AWS::ApiGateway::Deployment",
                "Properties": {"RestApiId": {"Ref": "Rest"}},
            },
            "Stage": {
                "Type": "AWS::ApiGateway::Stage",
                "Properties": {
                    "RestApiId": {"Ref": "Rest"},
                    "StageName": "prod",
                },
            },
            "DefaultOPTIONS": {
                "Type": "AWS::ApiGateway::Method",
                "Properties": {
                    "HttpMethod": "OPTIONS",
                    "RestApiId": {"Ref": "Rest"},
                    "ResourceId": CFNTag("GetAtt", ["Rest", "RootResourceId"]),
                    "Integration": {
                        "Type": "MOCK",
                        "IntegrationResponses": [
                            {
                                "StatusCode": "204",
                                "ResponseParameters": {
                                    "method.response.header.Access-Control-Allow-Headers": "'Content-Type,X-Amz-Date,Authorization,X-Api-Key'",
                                    "method.response.header.Access-Control-Allow-Origin": "'*'",
                                    "method.response.header.Access-Control-Allow-Methods": "'OPTIONS,GET,POST'",
                                },
                            }
                        ],
                        "RequestTemplates": {
                            "application/json": "{ statusCode: 200 }",
                        },
                    },
                    "MethodResponses": [
                        {
                            "StatusCode": "204",
                            "ResponseParameters": {
                                "method.response.header.Access-Control-Allow-Headers": True,
                                "method.response.header.Access-Control-Allow-Origin": True,
                                "method.response.header.Access-Control-Allow-Methods": True,
                            },
                        }
                    ],
                },
            },
        },
        "Outputs": {
            "Endpoint": {
                "Value": CFNTag(
                    "Sub",
                    "https://${Rest}.execute-api.${AWS::Region}.${AWS::URLSuffix}/${Stage}/",
                )
            }
        },
    }

    updated, changed = samify_template(template, asset_search_paths=[], relative_to=tmp_path)

    assert changed
    resources = updated["Resources"]
    rest = resources["Rest"]
    assert rest["Type"] == "AWS::Serverless::Api"
    props = rest["Properties"]
    assert props.get("StageName") == "prod"
    cors = props.get("Cors")
    assert cors is not None
    assert cors["AllowOrigin"] == "'*'"
    assert cors["AllowHeaders"] == "'Content-Type,X-Amz-Date,Authorization,X-Api-Key'"
    assert cors["AllowMethods"] == "'OPTIONS,GET,POST'"
    assert "Deployment" not in resources
    assert "Stage" not in resources
    assert "DefaultOPTIONS" not in resources

    endpoint = updated["Outputs"]["Endpoint"]["Value"]
    assert isinstance(endpoint, CFNTag)
    assert endpoint.tag == "Sub"
    text = endpoint.value[0] if isinstance(endpoint.value, list) else endpoint.value
    assert "${Stage}" not in text
    assert "/prod/" in text


def test_rest_api_shell_skips_when_referenced(tmp_path):
    template = {
        "Resources": {
            "Rest": {"Type": "AWS::ApiGateway::RestApi", "Properties": {"Name": "api"}},
            "Deployment": {
                "Type": "AWS::ApiGateway::Deployment",
                "Properties": {"RestApiId": {"Ref": "Rest"}},
            },
            "Custom": {
                "Type": "Custom::Watcher",
                "Properties": {"Target": {"Ref": "Deployment"}},
            },
        }
    }

    updated, changed = samify_template(template, asset_search_paths=[], relative_to=tmp_path)

    resources = updated["Resources"]
    assert not changed or resources.get("Rest", {}).get("Type") == "AWS::ApiGateway::RestApi"
    assert "Deployment" in resources


def test_state_machine_converts_by_default(tmp_path):
    template = {
        "Resources": {
            "MyState": {
                "Type": "AWS::StepFunctions::StateMachine",
                "Properties": {
                    "DefinitionString": "{}",
                    "StateMachineName": "demo",
                },
            }
        }
    }

    updated, changed = samify_template(template, asset_search_paths=[], relative_to=tmp_path)
    resources = updated["Resources"]
    sm = resources["MyState"]
    assert sm["Type"] == "AWS::Serverless::StateMachine"
    props = sm["Properties"]
    assert props.get("Name") == "demo"
    assert "Definition" in props


def test_appsync_converts_without_cdk_clean(tmp_path):
    template = {
        "Resources": {
            "Api": {
                "Type": "AWS::AppSync::GraphQLApi",
                "Properties": {
                    "AuthenticationType": "API_KEY",
                    "Name": "cars",
                },
            },
            "Schema": {
                "Type": "AWS::AppSync::GraphQLSchema",
                "Properties": {
                    "ApiId": {"Fn::GetAtt": ["Api", "ApiId"]},
                    "Definition": "schema { query: Query } type Query { ping: String }",
                },
            },
            "Source": {
                "Type": "AWS::AppSync::DataSource",
                "Properties": {
                    "ApiId": {"Fn::GetAtt": ["Api", "ApiId"]},
                    "Name": "Ping",
                    "Type": "AWS_LAMBDA",
                    "LambdaConfig": {"LambdaFunctionArn": {"Fn::GetAtt": ["Fn", "Arn"]}},
                },
            },
            "Function": {
                "Type": "AWS::AppSync::FunctionConfiguration",
                "Properties": {
                    "ApiId": {"Fn::GetAtt": ["Api", "ApiId"]},
                    "Name": "PingFn",
                    "DataSourceName": "Ping",
                    "Runtime": {"Name": "APPSYNC_JS", "RuntimeVersion": "1.0.0"},
                    "Code": "export const request = () => ({}); export const response = () => 'ok';",
                },
            },
            "Resolver": {
                "Type": "AWS::AppSync::Resolver",
                "Properties": {
                    "ApiId": {"Fn::GetAtt": ["Api", "ApiId"]},
                    "TypeName": "Query",
                    "FieldName": "ping",
                    "Kind": "PIPELINE",
                    "PipelineConfig": {"Functions": [{"Fn::GetAtt": ["Function", "FunctionId"]}]},
                    "Runtime": {"Name": "APPSYNC_JS", "RuntimeVersion": "1.0.0"},
                    "Code": "export const response = () => 'ok';",
                },
            },
            "Fn": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Handler": "index.handler",
                    "Runtime": "nodejs22.x",
                    "Code": {"ZipFile": "exports.handler = () => 'ok';"},
                },
            },
        }
    }

    updated, changed = samify_template(template, asset_search_paths=[], relative_to=tmp_path)
    resources = updated["Resources"]
    api = resources["Api"]
    assert api["Type"] == "AWS::Serverless::GraphQLApi"
    props = api["Properties"]
    assert props.get("Name") == "cars"
    assert "SchemaInline" in props or "SchemaUri" in props
    assert "Resolvers" in props


def test_samify_preserves_resource_inline_comment_on_lambda(tmp_path):
    """When converting a Lambda function to SAM, keep inline comments on Type."""

    yaml = create_cfn_yaml()
    template = yaml.load(
        """
Resources:
  DemoFn:
    Type: AWS::Lambda::Function  # important function comment
    Properties:
      Code:
        ZipFile: exports.handler = () => null
      Handler: index.handler
      Runtime: nodejs22.x
"""
    )

    updated, changed = samify_template(template, asset_search_paths=[], relative_to=tmp_path)
    assert changed

    # Render back to YAML and ensure the inline comment survived
    dump_yaml = create_cfn_yaml()
    buf = io.StringIO()
    dump_yaml.dump(updated, buf)
    rendered = buf.getvalue()

    assert "AWS::Serverless::Function" in rendered
    assert "# important function comment" in rendered


def test_s3_policy_template_conversion(tmp_path):
    template = {
        "Resources": {
            "Files": {"Type": "AWS::S3::Bucket"},
            "Role": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "AssumeRolePolicyDocument": {
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": "sts:AssumeRole",
                                "Principal": {"Service": "lambda.amazonaws.com"},
                            }
                        ]
                    },
                    "ManagedPolicyArns": [
                        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
                    ],
                },
            },
            "RolePolicy": {
                "Type": "AWS::IAM::Policy",
                "Properties": {
                    "PolicyName": "FilesRead",
                    "Roles": [{"Ref": "Role"}],
                    "PolicyDocument": {
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": ["s3:GetObject", "s3:ListBucket"],
                                "Resource": [
                                    {"Fn::GetAtt": ["Files", "Arn"]},
                                    {"Fn::Sub": "${Files.Arn}/*"},
                                ],
                            }
                        ]
                    },
                },
            },
            "Fn": {
                "Type": "AWS::Lambda::Function",
                "DependsOn": ["Role"],
                "Properties": {
                    "Handler": "index.handler",
                    "Runtime": "python3.12",
                    "Code": {"ZipFile": "def handler(event, ctx):\n  return 'ok'"},
                    "Role": {"Fn::GetAtt": ["Role", "Arn"]},
                },
            },
        }
    }

    updated, changed = samify_template(template, asset_search_paths=[], relative_to=tmp_path)
    resources = updated["Resources"]
    fn = resources["Fn"]
    policies = fn["Properties"].get("Policies")
    assert {"S3ReadPolicy": {"BucketName": {"Ref": "Files"}}} in policies
    assert "RolePolicy" not in resources
    assert "Role" not in resources
    assert "Role" not in fn["Properties"].get("DependsOn", [])


def test_sqs_policy_template_conversion(tmp_path):
    template = {
        "Resources": {
            "Q": {"Type": "AWS::SQS::Queue"},
            "Role": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "AssumeRolePolicyDocument": {
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": "sts:AssumeRole",
                                "Principal": {"Service": "lambda.amazonaws.com"},
                            }
                        ]
                    },
                    "ManagedPolicyArns": [
                        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
                    ],
                },
            },
            "PollerPolicy": {
                "Type": "AWS::IAM::Policy",
                "Properties": {
                    "PolicyName": "PollQ",
                    "Roles": [{"Ref": "Role"}],
                    "PolicyDocument": {
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "sqs:ReceiveMessage",
                                    "sqs:DeleteMessage",
                                    "sqs:GetQueueAttributes",
                                    "sqs:GetQueueUrl",
                                    "sqs:ChangeMessageVisibility",
                                ],
                                "Resource": {"Fn::GetAtt": ["Q", "Arn"]},
                            }
                        ]
                    },
                },
            },
            "Fn": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Handler": "index.handler",
                    "Runtime": "python3.12",
                    "Code": {"ZipFile": "def handler(event, ctx):\n  return 'ok'"},
                    "Role": {"Fn::GetAtt": ["Role", "Arn"]},
                },
            },
        }
    }

    updated, _ = samify_template(template, asset_search_paths=[], relative_to=tmp_path)
    resources = updated["Resources"]
    fn = resources["Fn"]
    policies = fn["Properties"].get("Policies")
    assert {"SQSPollerPolicy": {"QueueName": {"Ref": "Q"}}} in policies
    assert "PollerPolicy" not in resources


def test_simple_table_converts_when_provisioned(tmp_path):
    template = {
        "Resources": {
            "Table": {
                "Type": "AWS::DynamoDB::Table",
                "Properties": {
                    "AttributeDefinitions": [
                        {"AttributeName": "id", "AttributeType": "S"},
                    ],
                    "KeySchema": [
                        {"AttributeName": "id", "KeyType": "HASH"},
                    ],
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 5,
                        "WriteCapacityUnits": 5,
                    },
                    "TableName": "my-table",
                },
            }
        }
    }

    updated, _ = samify_template(template, asset_search_paths=[], relative_to=tmp_path)
    resources = updated["Resources"]
    table = resources["Table"]
    assert table["Type"] == "AWS::Serverless::SimpleTable"
    props = table["Properties"]
    assert props["PrimaryKey"] == {"Name": "id", "Type": "String"}
    assert props["ProvisionedThroughput"] == {
        "ReadCapacityUnits": 5,
        "WriteCapacityUnits": 5,
    }
    assert props["TableName"] == "my-table"


def test_simple_table_skips_on_demand(tmp_path):
    template = {
        "Resources": {
            "Table": {
                "Type": "AWS::DynamoDB::Table",
                "Properties": {
                    "AttributeDefinitions": [
                        {"AttributeName": "id", "AttributeType": "S"},
                    ],
                    "KeySchema": [
                        {"AttributeName": "id", "KeyType": "HASH"},
                    ],
                    "BillingMode": "PAY_PER_REQUEST",
                },
            }
        }
    }

    updated, _ = samify_template(template, asset_search_paths=[], relative_to=tmp_path)
    resources = updated["Resources"]
    assert resources["Table"]["Type"] == "AWS::DynamoDB::Table"


def test_http_api_shell_converts_when_orphaned(tmp_path):
    template = {
        "Resources": {
            "Http": {
                "Type": "AWS::ApiGatewayV2::Api",
                "Properties": {
                    "Name": "api",
                    "Description": "desc",
                    "Body": {"openapi": "3.0.1", "paths": {}},
                },
            },
            "Stage": {
                "Type": "AWS::ApiGatewayV2::Stage",
                "Properties": {"ApiId": {"Ref": "Http"}, "StageName": "$default"},
            },
        }
    }

    updated, changed = samify_template(template, asset_search_paths=[], relative_to=tmp_path)

    resources = updated["Resources"]
    http = resources["Http"]
    assert http["Type"] == "AWS::Serverless::HttpApi"
    props = http["Properties"]
    assert props["Name"] == "api"
    assert props["Description"] == "desc"
    assert "DefinitionBody" in props
    assert "Stage" not in resources


def test_http_api_shell_skips_when_routes_exist(tmp_path):
    template = {
        "Resources": {
            "Http": {"Type": "AWS::ApiGatewayV2::Api", "Properties": {"Name": "api"}},
            "Route": {
                "Type": "AWS::ApiGatewayV2::Route",
                "Properties": {"ApiId": {"Ref": "Http"}, "RouteKey": "GET /"},
            },
        }
    }

    updated, changed = samify_template(template, asset_search_paths=[], relative_to=tmp_path)

    resources = updated["Resources"]
    assert resources["Http"]["Type"] == "AWS::ApiGatewayV2::Api"
    assert "Route" in resources


def test_layer_version_converts_with_s3_content(tmp_path):
    template = {
        "Resources": {
            "MyLayer": {
                "Type": "AWS::Lambda::LayerVersion",
                "Properties": {
                    "Content": {
                        "S3Bucket": "bucket",
                        "S3Key": "layers/layer.zip",
                        "S3ObjectVersion": "1",
                    },
                    "Description": "layer",
                    "CompatibleRuntimes": ["nodejs22.x"],
                    "CompatibleArchitectures": ["x86_64"],
                },
            }
        }
    }

    updated, changed = samify_template(template, asset_search_paths=[], relative_to=tmp_path)
    assert changed
    layer = updated["Resources"]["MyLayer"]
    assert layer["Type"] == "AWS::Serverless::LayerVersion"
    props = layer["Properties"]
    assert props["ContentUri"] == {"Bucket": "bucket", "Key": "layers/layer.zip", "Version": "1"}
    assert props["CompatibleRuntimes"] == ["nodejs22.x"]
    assert props["CompatibleArchitectures"] == ["x86_64"]


def test_layer_version_skips_without_content(tmp_path):
    template = {
        "Resources": {
            "MyLayer": {
                "Type": "AWS::Lambda::LayerVersion",
                "Properties": {
                    "LayerName": "layer",
                },
            }
        }
    }

    updated, changed = samify_template(template, asset_search_paths=[], relative_to=tmp_path)
    assert not changed
    assert updated["Resources"]["MyLayer"]["Type"] == "AWS::Lambda::LayerVersion"


def test_iot_rule_converts(tmp_path):
    template = {
        "Resources": {
            "Fn": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {"ZipFile": "exports.handler = async () => {}"},
                    "Handler": "index.handler",
                    "Runtime": "nodejs22.x",
                },
            },
            "Rule": {
                "Type": "AWS::IoT::TopicRule",
                "Properties": {
                    "TopicRulePayload": {
                        "Sql": "SELECT * FROM 'topic'",
                        "RuleDisabled": False,
                    },
                    "Actions": [
                        {"Lambda": {"FunctionArn": {"Fn::GetAtt": ["Fn", "Arn"]}}},
                    ],
                },
            },
            "InvokePerm": {
                "Type": "AWS::Lambda::Permission",
                "Properties": {
                    "FunctionName": {"Ref": "Fn"},
                    "Principal": "iot.amazonaws.com",
                },
            },
        }
    }

    updated, changed = samify_template(template, asset_search_paths=[], relative_to=tmp_path)

    assert changed
    resources = updated["Resources"]
    fn = resources["Fn"]
    events = fn["Properties"].get("Events") or {}
    assert "Rule" in events
    ev = events["Rule"]
    assert ev["Type"] == "IoTRule"
    props = ev["Properties"]
    assert props["Sql"] == "SELECT * FROM 'topic'"
    assert props["RuleDisabled"] is False
    assert "Rule" not in resources
    assert "InvokePerm" not in resources


def test_cognito_trigger_converts(tmp_path):
    template = {
        "Resources": {
            "Fn": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {"ZipFile": "exports.handler = async () => {}"},
                    "Handler": "index.handler",
                    "Runtime": "nodejs22.x",
                },
            },
            "Pool": {
                "Type": "AWS::Cognito::UserPool",
                "Properties": {
                    "LambdaConfig": {
                        "PreSignUp": {"Ref": "Fn"},
                        "CustomMessage": {"Ref": "Fn"},
                    }
                },
            },
        }
    }

    updated, changed = samify_template(template, asset_search_paths=[], relative_to=tmp_path)

    assert changed
    resources = updated["Resources"]
    fn = resources["Fn"]
    events = fn["Properties"].get("Events") or {}
    assert "PoolPreSignUp" in events
    assert "PoolCustomMessage" in events
    ev = events["PoolPreSignUp"]
    assert ev["Type"] == "Cognito"
    assert ev["Properties"] == {"UserPool": {"Ref": "Pool"}, "Trigger": "PreSignUp"}
    pool = resources["Pool"]
    assert "LambdaConfig" not in pool.get("Properties", {})


def test_appsync_api_key_refs_after_cdk_clean_and_samify(tmp_path):
    template = {
        "Resources": {
            "TaskApiAF5FA34D": {
                "Type": "AWS::AppSync::GraphQLApi",
                "Properties": {"AuthenticationType": "API_KEY", "Name": "task-api"},
            },
            "TaskApiSchema02A3F420": {
                "Type": "AWS::AppSync::GraphQLSchema",
                "Properties": {
                    "ApiId": {"Fn::GetAtt": ["TaskApiAF5FA34D", "ApiId"]},
                    "Definition": "schema { query: Query } type Query { ping: String }",
                },
            },
            "TaskApiTasksDataSource4EE74D49": {
                "Type": "AWS::AppSync::DataSource",
                "Properties": {
                    "ApiId": {"Fn::GetAtt": ["TaskApiAF5FA34D", "ApiId"]},
                    "Name": "Tasks",
                    "Type": "AMAZON_DYNAMODB",
                    "DynamoDBConfig": {"TableName": "Tasks"},
                },
            },
            "TaskApiPingFunctionE250B1A0": {
                "Type": "AWS::AppSync::FunctionConfiguration",
                "Properties": {
                    "ApiId": {"Fn::GetAtt": ["TaskApiAF5FA34D", "ApiId"]},
                    "Name": "pingFn",
                    "DataSourceName": "Tasks",
                    "Runtime": {"Name": "APPSYNC_JS", "RuntimeVersion": "1.0.0"},
                    "Code": "export const request = () => ({}); export const response = () => 'ok';",
                },
            },
            "TaskApiDefaultApiKeyA6EE7DF9": {
                "Type": "AWS::AppSync::ApiKey",
                "Properties": {
                    "ApiId": {"Fn::GetAtt": ["TaskApiAF5FA34D", "ApiId"]},
                },
            },
            "TaskApiQueryPingResolverFE23C911": {
                "Type": "AWS::AppSync::Resolver",
                "Properties": {
                    "ApiId": {"Fn::GetAtt": ["TaskApiAF5FA34D", "ApiId"]},
                    "TypeName": "Query",
                    "FieldName": "ping",
                    "Kind": "PIPELINE",
                    "PipelineConfig": {
                        "Functions": [{"Fn::GetAtt": ["TaskApiPingFunctionE250B1A0", "FunctionId"]}]
                    },
                    "Runtime": {"Name": "APPSYNC_JS", "RuntimeVersion": "1.0.0"},
                    "Code": "export const response = () => 'ok';",
                },
            },
        },
        "Outputs": {
            "GraphQLAPIKey": {
                "Value": {"Fn::GetAtt": ["TaskApiAF5FA34DTaskApiDefaultApiKeyA6EE7DF9", "ApiKey"]}
            }
        },
    }

    source = TemplateSource(inline_content=yaml.safe_dump(template, sort_keys=False))
    options = TemplateProcessingOptions(
        cdk_clean=True,
        cdk_samify=True,
    )
    result = process_template(source, options)

    assert "AWS::Serverless::GraphQLApi" in result.formatted_content
    assert "TaskApiAF5FA34DTaskApiDefaultApiKeyA6EE7DF9" not in result.formatted_content


def test_samify_converts_inline_lambda(tmp_path):
    template = {
        "Resources": {
            "InlineFunc": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {"ZipFile": "exports.handler = () => 'hi'"},
                    "Handler": "index.handler",
                    "Runtime": "nodejs22.x",
                },
            },
            "Metadata": {
                "aws:asset:path": "asset.unknown",
                "aws:asset:property": "Code",
            },
        }
    }

    updated, changed = samify_template(
        template,
        asset_search_paths=[],
        relative_to=tmp_path,
    )

    assert changed
    func = updated["Resources"]["InlineFunc"]
    assert func["Type"] == "AWS::Serverless::Function"
    inline = func["Properties"]["InlineCode"]
    assert inline.strip().startswith("exports.handler")
    assert "\n" not in inline


def test_samify_converts_apigw_methods(tmp_path):
    template = {
        "Resources": {
            "Api": {
                "Type": "AWS::ApiGateway::RestApi",
                "Properties": {"Name": "Test"},
            },
            "RootMethod": {
                "Type": "AWS::ApiGateway::Method",
                "Properties": {
                    "HttpMethod": "GET",
                    "RestApiId": {"Ref": "Api"},
                    "ResourceId": CFNTag("GetAtt", ["Api", "RootResourceId"]),
                    "Integration": {
                        "Type": "AWS_PROXY",
                        "Uri": CFNTag(
                            "Sub",
                            "arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${InlineFunc.Arn}/invocations",
                        ),
                    },
                },
            },
            "ApiPermission": {
                "Type": "AWS::Lambda::Permission",
                "Properties": {
                    "Principal": "apigateway.amazonaws.com",
                    "Action": "lambda:InvokeFunction",
                    "FunctionName": {"Ref": "InlineFunc"},
                    "SourceArn": CFNTag(
                        "Sub",
                        "arn:aws:execute-api:${AWS::Region}:${AWS::AccountId}:${Api}/*/GET",
                    ),
                },
                "Metadata": {"aws:cdk:path": "Stack/ApiGateway/Default/GET/ApiPermission"},
            },
            "InlineFunc": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {"ZipFile": "exports.handler = () => 'hi'"},
                    "Handler": "index.handler",
                    "Runtime": "nodejs22.x",
                },
            },
            "Deployment": {
                "Type": "AWS::ApiGateway::Deployment",
                "DependsOn": ["RootMethod"],
                "Properties": {"RestApiId": {"Ref": "Api"}},
            },
        }
    }

    updated, changed = samify_template(
        template,
        asset_search_paths=[],
        relative_to=tmp_path,
    )

    assert changed
    resources = updated["Resources"]
    func = resources["InlineFunc"]
    inline = func["Properties"]["InlineCode"]
    assert inline.splitlines()[0].startswith("exports.handler")
    events = func["Properties"].get("Events")
    assert events
    event = next(iter(events.values()))
    assert event["Properties"]["Path"] == "/"
    assert "RootMethod" not in resources
    assert "ApiPermission" not in resources
    deployment = resources.get("Deployment")
    if deployment is not None:
        assert "RootMethod" not in deployment.get("DependsOn", [])


def test_samify_converts_apigw_methods_via_service(tmp_path):
    # Use a plain JSON payload without CFNTag so service pipeline can parse it
    raw = json.dumps(
        {
            "Resources": {
                "Api": {
                    "Type": "AWS::ApiGateway::RestApi",
                    "Properties": {"Name": "Test"},
                },
                "RootMethod": {
                    "Type": "AWS::ApiGateway::Method",
                    "Properties": {
                        "HttpMethod": "GET",
                        "RestApiId": {"Ref": "Api"},
                        "ResourceId": {"Fn::GetAtt": ["Api", "RootResourceId"]},
                        "Integration": {
                            "Type": "AWS_PROXY",
                            "Uri": {
                                "Fn::Sub": "arn:aws:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${InlineFunc.Arn}/invocations"
                            },
                        },
                    },
                },
                "ApiPermission": {
                    "Type": "AWS::Lambda::Permission",
                    "Properties": {
                        "Principal": "apigateway.amazonaws.com",
                        "Action": "lambda:InvokeFunction",
                        "FunctionName": {"Ref": "InlineFunc"},
                        "SourceArn": {
                            "Fn::Sub": "arn:aws:execute-api:${AWS::Region}:${AWS::AccountId}:${Api}/*/GET"
                        },
                    },
                    "Metadata": {"aws:cdk:path": "Stack/ApiGateway/Default/GET/ApiPermission"},
                },
                "InlineFunc": {
                    "Type": "AWS::Lambda::Function",
                    "Properties": {
                        "Code": {"ZipFile": "exports.handler = () => 'hi'"},
                        "Handler": "index.handler",
                        "Runtime": "nodejs22.x",
                    },
                },
                "Deployment": {
                    "Type": "AWS::ApiGateway::Deployment",
                    "DependsOn": ["RootMethod"],
                    "Properties": {"RestApiId": {"Ref": "Api"}},
                },
            }
        }
    )
    source = TemplateSource(inline_content=raw)
    options = TemplateProcessingOptions(
        cdk_clean=False,
        cdk_samify=True,
    )
    result = process_template(source, options)

    from pretty_cfn.formatter import CFNLoader

    rendered = CFNLoader(result.formatted_content).get_single_data()
    resources = rendered["Resources"]
    func = resources["InlineFunc"]
    assert func["Type"] == "AWS::Serverless::Function"
    events = func["Properties"].get("Events")
    assert events
    event = next(iter(events.values()))
    assert event["Type"] == "Api"
    assert event["Properties"]["Path"] == "/"
    # The original Method and permission should be removed in the SAM view
    assert "RootMethod" not in resources
    assert "ApiPermission" not in resources


def test_samify_externalizes_inline_code_when_preferred(tmp_path):
    template = {
        "Resources": {
            "InlineFunc": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {"ZipFile": "exports.handler = () => 'hi'"},
                    "Handler": "index.handler",
                    "Runtime": "nodejs20.x",
                },
            }
        }
    }

    stager = SamAssetStager(tmp_path, assets_subdir="src")
    updated, changed = samify_template(
        template,
        asset_search_paths=[],
        relative_to=tmp_path,
        asset_stager=stager,
        prefer_external_assets=True,
    )

    assert changed
    func = updated["Resources"]["InlineFunc"]
    props = func["Properties"]
    assert "InlineCode" not in props
    assert props["CodeUri"] == "src/InlineFunc"
    staged_file = tmp_path / "src" / "InlineFunc" / "index.js"
    assert staged_file.exists()
    assert "exports.handler" in staged_file.read_text()


def test_samify_drops_basic_lambda_role(tmp_path):
    template = {
        "Resources": {
            "MyRole": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "AssumeRolePolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": {"Service": "lambda.amazonaws.com"},
                                "Action": "sts:AssumeRole",
                            }
                        ],
                    },
                    "ManagedPolicyArns": [
                        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
                    ],
                },
            },
            "MyFunc": {
                "Type": "AWS::Lambda::Function",
                "DependsOn": ["MyRole"],
                "Properties": {
                    "Code": {
                        "S3Bucket": {"Ref": "Assets"},
                        "S3Key": "asset.zip",
                    },
                    "Handler": "index.handler",
                    "Runtime": "nodejs22.x",
                    "Role": {"Fn::GetAtt": ["MyRole", "Arn"]},
                },
                "Metadata": {
                    "aws:asset:path": "asset.123",
                    "aws:asset:property": "Code",
                },
            },
        }
    }

    asset_dir = tmp_path / "cdk.out"
    asset_path = asset_dir / "asset.123"
    asset_path.mkdir(parents=True)

    updated, changed = samify_template(
        template,
        asset_search_paths=[asset_dir],
        relative_to=tmp_path,
    )

    assert changed
    resources = updated["Resources"]
    assert "MyRole" not in resources
    func = resources["MyFunc"]
    assert func["Type"] == "AWS::Serverless::Function"
    assert "Role" not in func["Properties"]
    assert "DependsOn" not in func


def test_samify_downloads_s3_assets(tmp_path):
    template = {
        "Resources": {
            "S3Func": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {
                        "S3Bucket": CFNTag("Sub", "bucket-${AWS::Region}"),
                        "S3Key": "code.zip",
                    },
                    "Handler": "app.handler",
                    "Runtime": "python3.12",
                },
                "Metadata": {
                    "aws:asset:path": "asset.missing",
                    "aws:asset:property": "Code",
                },
            }
        }
    }

    artifact = tmp_path / "artifact.zip"
    with zipfile.ZipFile(artifact, "w") as zip_file:
        zip_file.writestr("app.py", "def handler(event, context):\n    return 'ok'\n")

    def fake_download(bucket: str, key: str, version: Optional[str], target: Path):
        shutil.copy2(artifact, target)

    env = AwsEnvironment(account_id="123456789012", region="us-east-1", partition="aws")
    stager = SamAssetStager(tmp_path, assets_subdir="src", s3_downloader=fake_download, aws_env=env)

    updated, changed = samify_template(
        template,
        asset_search_paths=[],
        relative_to=tmp_path,
        asset_stager=stager,
    )

    assert changed
    func = updated["Resources"]["S3Func"]
    assert func["Type"] == "AWS::Serverless::Function"
    assert func["Properties"]["CodeUri"] == "src/S3Func"
    handler_path = tmp_path / "src" / "S3Func" / "app.py"
    assert handler_path.exists()


def test_samify_downloads_s3_assets_with_dict_sub(tmp_path):
    template = {
        "Resources": {
            "S3Func": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {
                        "S3Bucket": {"Fn::Sub": "bucket-${AWS::Region}"},
                        "S3Key": {"Fn::Sub": "code-${AWS::AccountId}.zip"},
                    },
                    "Handler": "app.handler",
                    "Runtime": "python3.12",
                },
            }
        }
    }

    artifact = tmp_path / "artifact.zip"
    with zipfile.ZipFile(artifact, "w") as zip_file:
        zip_file.writestr("app.py", "def handler(event, context):\n    return 'ok'\n")

    def fake_download(bucket: str, key: str, version: Optional[str], target: Path):
        assert bucket == "bucket-us-east-1"
        assert key == "code-123456789012.zip"
        shutil.copy2(artifact, target)

    env = AwsEnvironment(account_id="123456789012", region="us-east-1", partition="aws")
    stager = SamAssetStager(tmp_path, assets_subdir="src", s3_downloader=fake_download, aws_env=env)

    updated, changed = samify_template(
        template,
        asset_search_paths=[],
        relative_to=tmp_path,
        asset_stager=stager,
    )

    assert changed
    func = updated["Resources"]["S3Func"]
    assert func["Type"] == "AWS::Serverless::Function"
    assert func["Properties"]["CodeUri"] == "src/S3Func"
    handler_path = tmp_path / "src" / "S3Func" / "app.py"
    assert handler_path.exists()


def test_samify_keeps_websocket_api_gateway_resources(tmp_path):
    template = {
        "Resources": {
            "ChatApi": {
                "Type": "AWS::ApiGatewayV2::Api",
                "Properties": {
                    "ProtocolType": "WEBSOCKET",
                    "RouteSelectionExpression": "$request.body.action",
                    "Name": "ChatApi",
                },
            },
            "ConnectLambda": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Handler": "index.handler",
                    "Runtime": "nodejs22.x",
                    "Code": {"ZipFile": "exports.handler = async () => {};"},
                },
            },
            "ConnectIntegration": {
                "Type": "AWS::ApiGatewayV2::Integration",
                "Properties": {
                    "ApiId": {"Ref": "ChatApi"},
                    "IntegrationType": "AWS_PROXY",
                    "IntegrationUri": {
                        "Fn::Sub": "arn:aws:apigateway:us-east-2:lambda:path/2015-03-31/functions/${ConnectLambda.Arn}/invocations",
                    },
                },
            },
            "ConnectRoute": {
                "Type": "AWS::ApiGatewayV2::Route",
                "Properties": {
                    "ApiId": {"Ref": "ChatApi"},
                    "RouteKey": "$connect",
                    "Target": {"Fn::Sub": "integrations/${ConnectIntegration}"},
                },
            },
            "ChatDeployment": {
                "Type": "AWS::ApiGatewayV2::Deployment",
                "Properties": {"ApiId": {"Ref": "ChatApi"}},
            },
            "ChatStage": {
                "Type": "AWS::ApiGatewayV2::Stage",
                "Properties": {
                    "ApiId": {"Ref": "ChatApi"},
                    "StageName": "dev",
                    "DeploymentId": {"Ref": "ChatDeployment"},
                },
            },
        }
    }

    updated, changed = samify_template(template, asset_search_paths=[], relative_to=tmp_path)

    assert changed
    api = updated["Resources"]["ChatApi"]
    assert api["Type"] == "AWS::ApiGatewayV2::Api"
    connect_function = updated["Resources"]["ConnectLambda"]
    assert connect_function["Type"] == "AWS::Serverless::Function"
    assert "ConnectIntegration" in updated["Resources"]
    assert "ConnectRoute" in updated["Resources"]


def test_samify_moves_iam_policies_into_function(tmp_path):
    template = {
        "Resources": {
            "Connections": {
                "Type": "AWS::DynamoDB::Table",
                "Properties": {
                    "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                    "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
                },
            },
            "WorkerRole": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "AssumeRolePolicyDocument": {
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": "sts:AssumeRole",
                                "Principal": {"Service": "lambda.amazonaws.com"},
                            }
                        ],
                    },
                    "ManagedPolicyArns": [
                        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
                    ],
                },
            },
            "WorkerPolicy": {
                "Type": "AWS::IAM::Policy",
                "Properties": {
                    "PolicyName": "Worker",
                    "Roles": [{"Ref": "WorkerRole"}],
                    "PolicyDocument": {
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "dynamodb:BatchGetItem",
                                    "dynamodb:GetItem",
                                    "dynamodb:PutItem",
                                    "dynamodb:UpdateItem",
                                ],
                                "Resource": [{"Fn::GetAtt": ["Connections", "Arn"]}],
                            }
                        ],
                    },
                },
            },
            "Worker": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Handler": "index.handler",
                    "Runtime": "nodejs22.x",
                    "Role": {"Fn::GetAtt": ["WorkerRole", "Arn"]},
                    "Code": {"ZipFile": "exports.handler = async () => {};"},
                },
            },
        }
    }

    updated, _ = samify_template(template, asset_search_paths=[], relative_to=tmp_path)
    worker = updated["Resources"]["Worker"]
    policies = worker["Properties"].get("Policies")
    assert policies
    assert {"DynamoDBCrudPolicy": {"TableName": {"Ref": "Connections"}}} in policies
    assert "Role" not in worker["Properties"]
    assert "WorkerRole" not in updated["Resources"]
    assert "WorkerPolicy" not in updated["Resources"]


def test_samify_preserves_manage_connections_statement(tmp_path):
    template = {
        "Resources": {
            "ChatApi": {
                "Type": "AWS::ApiGatewayV2::Api",
                "Properties": {
                    "ProtocolType": "WEBSOCKET",
                    "RouteSelectionExpression": "$request.body.action",
                },
            },
            "WorkerRole": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "AssumeRolePolicyDocument": {
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": "sts:AssumeRole",
                                "Principal": {"Service": "lambda.amazonaws.com"},
                            }
                        ],
                    },
                    "ManagedPolicyArns": [
                        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
                    ],
                },
            },
            "WorkerPolicy": {
                "Type": "AWS::IAM::Policy",
                "Properties": {
                    "PolicyName": "Worker",
                    "Roles": [{"Ref": "WorkerRole"}],
                    "PolicyDocument": {
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": "execute-api:ManageConnections",
                                "Resource": {
                                    "Fn::Sub": "arn:aws:execute-api:us-east-2::${ChatApi}/*"
                                },
                            },
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "dynamodb:GetItem",
                                    "dynamodb:PutItem",
                                    "dynamodb:UpdateItem",
                                ],
                                "Resource": [{"Fn::GetAtt": ["Connections", "Arn"]}],
                            },
                        ]
                    },
                },
            },
            "Connections": {
                "Type": "AWS::DynamoDB::Table",
                "Properties": {
                    "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                    "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
                },
            },
            "Worker": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Handler": "index.handler",
                    "Runtime": "nodejs22.x",
                    "Role": {"Fn::GetAtt": ["WorkerRole", "Arn"]},
                    "Code": {"ZipFile": "exports.handler = async () => {};"},
                },
            },
        }
    }

    updated, _ = samify_template(template, asset_search_paths=[], relative_to=tmp_path)
    policies = updated["Resources"]["Worker"]["Properties"].get("Policies")
    assert any(
        isinstance(entry, dict)
        and "Statement" in entry
        and any(
            stmt.get("Action") == "execute-api:ManageConnections"
            or "execute-api:ManageConnections" in stmt.get("Action", [])
            for stmt in (
                entry["Statement"] if isinstance(entry["Statement"], list) else [entry["Statement"]]
            )
        )
        for entry in policies
    )


def test_convert_appsync_prefers_external_assets(tmp_path):
    template = {
        "Resources": {
            "CarApi": {
                "Type": "AWS::AppSync::GraphQLApi",
                "Properties": {
                    "AuthenticationType": "API_KEY",
                    "Name": "cars",
                },
            },
            "GraphSchema": {
                "Type": "AWS::AppSync::GraphQLSchema",
                "Properties": {
                    "ApiId": CFNTag("GetAtt", ["CarApi", "ApiId"]),
                    "Definition": "schema {\n  query: Query\n}\n\ntype Query {\n  listCars: String\n}\n",
                },
            },
            "CarsSource": {
                "Type": "AWS::AppSync::DataSource",
                "Properties": {
                    "ApiId": CFNTag("GetAtt", ["CarApi", "ApiId"]),
                    "Name": "Cars",
                    "Type": "AMAZON_DYNAMODB",
                    "DynamoDBConfig": {"TableName": "CarsTable"},
                },
            },
            "CarsFunction": {
                "Type": "AWS::AppSync::FunctionConfiguration",
                "Properties": {
                    "ApiId": CFNTag("GetAtt", ["CarApi", "ApiId"]),
                    "Name": "listCars",
                    "DataSourceName": "Cars",
                    "Runtime": {"Name": "APPSYNC_JS", "RuntimeVersion": "1.0.0"},
                    "Code": "export const request = () => ({});",
                },
            },
            "CarsResolver": {
                "Type": "AWS::AppSync::Resolver",
                "Properties": {
                    "ApiId": CFNTag("GetAtt", ["CarApi", "ApiId"]),
                    "TypeName": "Query",
                    "FieldName": "listCars",
                    "Kind": "PIPELINE",
                    "PipelineConfig": {
                        "Functions": [CFNTag("GetAtt", ["CarsFunction", "FunctionId"])]
                    },
                    "Runtime": {"Name": "APPSYNC_JS", "RuntimeVersion": "1.0.0"},
                    "Code": "export const response = () => 'ok';",
                },
            },
        }
    }

    stager = SamAssetStager(tmp_path, assets_subdir="src")
    updated, changed = convert_appsync_apis(
        template,
        asset_search_paths=[],
        relative_to=tmp_path,
        asset_stager=stager,
        prefer_external_assets=True,
    )

    assert changed
    api = updated["Resources"]["CarApi"]
    props = api["Properties"]
    assert "SchemaInline" not in props
    schema_uri = props["SchemaUri"]
    assert schema_uri.startswith("src/")
    schema_path = tmp_path / schema_uri
    assert schema_path.exists()
    function_entry = props["Functions"]["CarsFunction"]
    assert "InlineCode" not in function_entry
    assert function_entry["CodeUri"].startswith("src/")
    resolver_entry = props["Resolvers"]["Query"]["CarsResolver"]
    assert resolver_entry["CodeUri"].startswith("src/")


def test_convert_appsync_rewrites_api_key_references(tmp_path):
    template = {
        "Resources": {
            "TaskApi": {
                "Type": "AWS::AppSync::GraphQLApi",
                "Properties": {
                    "AuthenticationType": "API_KEY",
                },
            },
            "TaskSchema": {
                "Type": "AWS::AppSync::GraphQLSchema",
                "Properties": {
                    "ApiId": CFNTag("GetAtt", ["TaskApi", "ApiId"]),
                    "Definition": "schema {\n  query: Query\n}\n\ntype Query {\n  ping: String\n}\n",
                },
            },
            "TaskSource": {
                "Type": "AWS::AppSync::DataSource",
                "Properties": {
                    "ApiId": CFNTag("GetAtt", ["TaskApi", "ApiId"]),
                    "Type": "AMAZON_DYNAMODB",
                    "Name": "Tasks",
                    "DynamoDBConfig": {"TableName": "Tasks"},
                },
            },
            "TaskFunction": {
                "Type": "AWS::AppSync::FunctionConfiguration",
                "Properties": {
                    "ApiId": CFNTag("GetAtt", ["TaskApi", "ApiId"]),
                    "Name": "listTasks",
                    "DataSourceName": "Tasks",
                    "Runtime": {"Name": "APPSYNC_JS", "RuntimeVersion": "1.0.0"},
                    "Code": "export const handler = () => [];",
                },
            },
            "TaskResolver": {
                "Type": "AWS::AppSync::Resolver",
                "Properties": {
                    "ApiId": CFNTag("GetAtt", ["TaskApi", "ApiId"]),
                    "TypeName": "Query",
                    "FieldName": "listTasks",
                    "Kind": "PIPELINE",
                    "PipelineConfig": {
                        "Functions": [CFNTag("GetAtt", ["TaskFunction", "FunctionId"])]
                    },
                    "Runtime": {"Name": "APPSYNC_JS", "RuntimeVersion": "1.0.0"},
                    "Code": "export const response = () => [];",
                },
            },
            "TaskApiKey": {
                "Type": "AWS::AppSync::ApiKey",
                "Properties": {
                    "ApiId": CFNTag("GetAtt", ["TaskApi", "ApiId"]),
                    "Description": "default",
                },
            },
            "KeyWatcher": {
                "Type": "Custom::Watcher",
                "DependsOn": ["TaskApiKey"],
            },
        },
        "Outputs": {
            "GraphQLKey": {
                "Value": CFNTag("GetAtt", ["TaskApiKey", "ApiKey"]),
            }
        },
    }

    updated, changed = convert_appsync_apis(
        template,
        asset_search_paths=[tmp_path],
        relative_to=tmp_path,
        asset_stager=None,
        prefer_external_assets=False,
    )

    assert changed
    api = updated["Resources"]["TaskApi"]
    api_keys = api["Properties"]["ApiKeys"]
    assert api_keys["TaskApiKey"]["ApiKeyId"] == "TaskApiKey"
    output_tag = updated["Outputs"]["GraphQLKey"]["Value"]
    assert isinstance(output_tag, CFNTag)
    assert output_tag.value[0] == "TaskApiTaskApiKey"
    assert "DependsOn" not in updated["Resources"]["KeyWatcher"]


def test_rewrite_function_url_refs_updates_outputs():
    template = {
        "Outputs": {
            "Url": {
                "Value": {
                    "Fn::GetAtt": ["OldFunctionUrl", "FunctionUrl"],
                }
            }
        }
    }

    rewrite_function_url_refs(template, {"OldFunctionUrl": "NewFunctionUrl"})

    value = template["Outputs"]["Url"]["Value"]["Fn::GetAtt"][0]
    assert value == "NewFunctionUrl"


def test_apply_function_globals_moves_shared_properties():
    template = {
        "Resources": {
            "FuncA": {
                "Type": "AWS::Serverless::Function",
                "Properties": {
                    "Runtime": "nodejs22.x",
                    "MemorySize": 256,
                    "Timeout": 30,
                    "Environment": {"Variables": {"TABLE_NAME": "Connections"}},
                },
            },
            "FuncB": {
                "Type": "AWS::Serverless::Function",
                "Properties": {
                    "Runtime": "nodejs22.x",
                    "MemorySize": 256,
                    "Timeout": 30,
                    "Environment": {"Variables": {"TABLE_NAME": "Connections"}},
                },
            },
        }
    }

    apply_function_globals(template)

    globals_block = template["Globals"]["Function"]
    assert globals_block["Runtime"] == "nodejs22.x"
    assert globals_block["Environment"]["Variables"]["TABLE_NAME"] == "Connections"
    for resource in template["Resources"].values():
        assert "Runtime" not in resource["Properties"]
        assert "Environment" not in resource["Properties"]


def test_strip_cdk_metadata_removes_bootstrap_parameter():
    template = {
        "Parameters": {
            "BootstrapVersion": {
                "Type": "AWS::SSM::Parameter::Value<String>",
                "Default": "/cdk-bootstrap/hnb659fds/version",
            }
        },
        "Resources": {
            "Fn": {
                "Type": "AWS::Serverless::Function",
                "Metadata": {"aws:cdk:path": "stack/Fn", "note": "keep"},
            }
        },
    }

    strip_cdk_metadata(template)

    assert "BootstrapVersion" not in template.get("Parameters", {})
    metadata = template["Resources"]["Fn"].get("Metadata")
    assert metadata == {"note": "keep"}


def test_convert_state_machine_to_serverless():
    template = {
        "Resources": {
            "Workflow": {
                "Type": "AWS::StepFunctions::StateMachine",
                "Properties": {
                    "StateMachineName": "Workflow",
                    "StateMachineType": "STANDARD",
                    "RoleArn": {"Fn::GetAtt": ["WorkflowRole", "Arn"]},
                    "LoggingConfiguration": {"Level": "ALL"},
                    "TracingConfiguration": {"Enabled": True},
                    "Definition": {
                        "StartAt": "Hello",
                        "States": {
                            "Hello": {"Type": "Succeed"},
                        },
                    },
                },
            }
        }
    }

    changed = convert_state_machines(template)

    assert changed
    resource = template["Resources"]["Workflow"]
    assert resource["Type"] == "AWS::Serverless::StateMachine"
    properties = resource["Properties"]
    assert properties["Name"] == "Workflow"
    assert properties["Type"] == "STANDARD"
    assert properties["Role"] == {"Fn::GetAtt": ["WorkflowRole", "Arn"]}
    assert properties["Logging"]["Level"] == "ALL"
    assert properties["Tracing"]["Enabled"] is True
    assert template["Transform"] == "AWS::Serverless-2016-10-31"


def test_convert_state_machine_handles_definition_string():
    template = {
        "Resources": {
            "Workflow": {
                "Type": "AWS::StepFunctions::StateMachine",
                "Properties": {
                    "RoleArn": "arn:aws:iam::123:role/foo",
                    "DefinitionString": json.dumps(
                        {
                            "StartAt": "Hi",
                            "States": {"Hi": {"Type": "Succeed"}},
                        }
                    ),
                },
            }
        }
    }

    changed = convert_state_machines(template)

    assert changed
    resource = template["Resources"]["Workflow"]
    assert resource["Type"] == "AWS::Serverless::StateMachine"
    assert "Definition" in resource["Properties"]
    assert "DefinitionString" not in resource["Properties"]
