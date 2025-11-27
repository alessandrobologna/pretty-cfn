"""Tests for the pretty-cfn formatter."""

import json

from pretty_cfn.formatter import (
    format_cfn_yaml,
    _align_values,
    _to_ordered_dict,
    create_cfn_yaml,
    CFNTag,
)


def test_basic_formatting():
    """Test basic YAML formatting with alignment."""
    input_yaml = """
AWSTemplateFormatVersion: '2010-09-09'
Description: Test template
Parameters:
  Param1:
    Type: String
    Default: value1
Resources:
  MyResource:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: my-bucket
"""

    result = format_cfn_yaml(input_yaml, alignment_column=40)

    # Check that the template is still valid YAML
    import yaml

    parsed = yaml.safe_load(result)
    assert parsed["AWSTemplateFormatVersion"] == "2010-09-09"
    assert parsed["Description"] == "Test template"
    assert "Parameters" in parsed
    assert "Resources" in parsed


def test_intrinsic_functions():
    """Test handling of CloudFormation intrinsic functions."""
    input_yaml = """
Resources:
  MyResource:
    Type: AWS::EC2::Instance
    Properties:
      ImageId: !Ref AMIId
      SubnetId: !GetAtt Network.SubnetId
      Tags:
        - Key: Name
          Value: !Sub '${AWS::StackName}-instance'
"""

    result = format_cfn_yaml(input_yaml, alignment_column=40)

    # Check that intrinsic functions are preserved
    assert "!Ref" in result
    assert "!GetAtt" in result
    assert "!Sub" in result


def test_alignment():
    """Test that values are properly aligned."""
    lines = [
        "AWSTemplateFormatVersion: '2010-09-09'",
        "Description: Test",
        "Parameters:",
        "  Name:",
        "    Type: String",
        "    Default: value",
    ]

    aligned = _align_values(lines, alignment_column=40)

    # Check alignment of the top-level keys
    assert aligned[0].index("'2010-09-09'") >= 40
    assert aligned[1].index("Test") >= 40


def test_ordered_dict_conversion():
    """Test that ordering is preserved and correct."""
    data = {
        "Resources": {"MyResource": {"Type": "AWS::S3::Bucket"}},
        "Parameters": {"Param1": {"Type": "String"}},
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Test",
    }

    ordered = _to_ordered_dict(data)

    # Check that keys are in the correct order
    keys = list(ordered.keys())
    assert keys[0] == "AWSTemplateFormatVersion"
    assert keys[1] == "Description"
    assert keys[2] == "Parameters"
    assert keys[3] == "Resources"


def test_multiline_strings():
    """Test handling of multi-line strings."""
    input_yaml = """
Resources:
  MyFunction:
    Type: AWS::Lambda::Function
    Properties:
      Code:
        ZipFile: |
          import json
          def handler(event, context):
              return {
                  'statusCode': 200,
                  'body': json.dumps('Hello')
              }
"""

    result = format_cfn_yaml(input_yaml, alignment_column=40)

    # Check that multi-line strings are preserved
    assert "import json" in result
    assert "def handler" in result


def test_lists():
    """Test handling of lists."""
    input_yaml = """
Resources:
  SecurityGroup:
    Type: AWS::EC2::SecurityGroup
    Properties:
      SecurityGroupIngress:
        - IpProtocol: tcp
          FromPort: 80
          ToPort: 80
          CidrIp: 0.0.0.0/0
        - IpProtocol: tcp
          FromPort: 443
          ToPort: 443
          CidrIp: 0.0.0.0/0
"""

    result = format_cfn_yaml(input_yaml, alignment_column=40)

    # Check that list structure is preserved
    import yaml

    parsed = yaml.safe_load(result)
    ingress = parsed["Resources"]["SecurityGroup"]["Properties"]["SecurityGroupIngress"]
    assert len(ingress) == 2
    assert ingress[0]["FromPort"] == 80
    assert ingress[1]["FromPort"] == 443


def test_stepfunctions_definitionstring_converted_to_definition():
    """DefinitionString built via Fn::Join should become a structured Definition."""

    template = {
        "Resources": {
            "WorkerFunction": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {"ZipFile": "exports.handler = () => null"},
                    "Handler": "index.handler",
                    "Role": "arn:aws:iam::123456789012:role/Dummy",
                    "Runtime": "nodejs20.x",
                },
            },
            "StateMachine": {
                "Type": "AWS::StepFunctions::StateMachine",
                "Properties": {
                    "DefinitionString": {
                        "Fn::Join": [
                            "",
                            [
                                '{"StartAt":"Task","States":{"Task":{"Type":"Task","Resource":"arn:',
                                {"Ref": "AWS::Partition"},
                                ':states:::lambda:invoke","Parameters":{"FunctionName":"',
                                {"Fn::GetAtt": ["WorkerFunction", "Arn"]},
                                '"}}}}',
                            ],
                        ]
                    },
                    "RoleArn": {"Fn::GetAtt": ["WorkerFunction", "Arn"]},
                },
            },
        }
    }

    result = format_cfn_yaml(json.dumps(template), alignment_column=40)
    yaml = create_cfn_yaml()
    parsed = yaml.load(result)
    props = parsed["Resources"]["StateMachine"]["Properties"]
    assert "DefinitionString" not in props
    definition = props["Definition"]
    task = definition["States"]["Task"]

    function_name = task["Parameters"]["FunctionName"]
    assert isinstance(function_name, CFNTag)
    assert function_name.tag == "GetAtt"

    resource_value = task["Resource"]
    assert isinstance(resource_value, CFNTag)
    assert resource_value.tag == "Sub"


def test_zipfile_block_no_leading_blank_and_dedented():
    """Lambda ZipFile strings should be emitted without extra blank line and with dedented body."""

    template = {
        "Resources": {
            "InlineFn": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Code": {
                        "ZipFile": "exports.handler = async (event) => {\n  return { statusCode: 200 };\n};\n",
                    },
                    "Handler": "index.handler",
                    "Runtime": "nodejs20.x",
                },
            }
        }
    }

    alignment_column = 40
    result = format_cfn_yaml(json.dumps(template), alignment_column=alignment_column)
    lines = result.splitlines()
    for idx, line in enumerate(lines):
        if "ZipFile:" in line:
            block_line = lines[idx]
            assert block_line.strip().endswith("|-") or block_line.strip().endswith("|")
            first_body_line = lines[idx + 1]
            # First body line should start at the value column and contain code immediately
            assert first_body_line.startswith(" " * alignment_column)
            assert first_body_line.strip() == "exports.handler = async (event) => {"
            second_body_line = lines[idx + 2]
            assert second_body_line.startswith(" " * (alignment_column + 2))
            assert second_body_line.strip() == "return { statusCode: 200 };"
            break
    else:
        assert False, "ZipFile block not found"


def test_json_input_roundtrip_intrinsics():
    """JSON input with Fn::GetAtt should round-trip to short form."""

    json_input = """
{
  "Resources": {
    "MyTopic": {
      "Type": "AWS::SNS::Topic"
    }
  },
  "Outputs": {
    "TopicArn": {
      "Value": {
        "Fn::GetAtt": ["MyTopic", "TopicArn"]
      }
    }
  }
}
"""

    result = format_cfn_yaml(json_input, alignment_column=40)
    assert "!GetAtt" in result


def test_contains_intrinsic_converted_to_short_form():
    """JSON-form Fn::Contains should become !Contains."""

    json_input = """
{
  "Conditions": {
    "HasFeature": {
      "Fn::Contains": [
        ["A", "B"],
        {"Ref": "FeatureFlag"}
      ]
    }
  }
}
"""

    result = format_cfn_yaml(json_input, alignment_column=40)
    assert "!Contains" in result


def test_list_indentation_under_keys():
    """Lists under a key should be indented two spaces."""
    input_yaml = """
Policy:
  Statement:
    Action:
      - s3:GetObject
      - s3:PutObject
"""
    result = format_cfn_yaml(input_yaml, alignment_column=40)
    # Ensure the list items are indented (two spaces) under Action:
    assert "Action:\n      -                                 s3:GetObject" in result


def test_list_scalar_alignment():
    """Scalar list items should align their values like other entries."""
    lines = [
        "      InstanceType:                     !If",
        "        - IsProduction",
        "        - t3.large",
        "        - t3.micro",
    ]

    aligned = _align_values(lines, alignment_column=40)

    # Each list value should start at or beyond the alignment column (40)
    assert aligned[1].index("IsProduction") >= 40
    assert aligned[2].index("t3.large") >= 40
    assert aligned[3].index("t3.micro") >= 40


def test_inline_mapping_in_list_alignment():
    """Inline mappings following '-' should align their values like other keys."""
    lines = [
        "      Tags:",
        "        - Key: Name",
        "          Value:                        !Ref 'Something'",
    ]

    aligned = _align_values(lines, alignment_column=40)

    assert "        - Key:                          Name" in aligned


def test_nested_list_not_aligned_as_scalar():
    """Nested sequences should keep the child dash tight to their parent."""
    lines = [
        "          Value:                        !Join",
        "            - '-'",
        "            - - !Ref 'AWS::StackName'",
        "              - !Ref 'Environment'",
        "              - instance",
    ]

    aligned = _align_values(lines, alignment_column=40)

    assert "            - -                         !Ref 'AWS::StackName'" in aligned
    assert "              -                         !Ref 'Environment'" in aligned
    assert "              -                         instance" in aligned


def test_empty_values():
    """Test handling of empty/null values."""
    input_yaml = """
Parameters:
  Param1:
    Type: String
    Default: ''
  Param2:
    Type: String
    Default:
"""

    result = format_cfn_yaml(input_yaml, alignment_column=40)

    # Check that empty values are handled correctly
    import yaml

    parsed = yaml.safe_load(result)
    assert parsed["Parameters"]["Param1"]["Default"] == ""
    assert (
        parsed["Parameters"]["Param2"]["Default"] is None
        or parsed["Parameters"]["Param2"]["Default"] == ""
    )


def test_special_strings():
    """Test handling of strings that need quotes."""
    input_yaml = """
Parameters:
  BoolString:
    Type: String
    Default: 'true'
  NumberString:
    Type: String
    Default: '123'
  YesString:
    Type: String
    Default: 'yes'
"""

    result = format_cfn_yaml(input_yaml, alignment_column=40)

    # These should remain as strings, not be converted to booleans/numbers
    import yaml

    parsed = yaml.safe_load(result)
    assert parsed["Parameters"]["BoolString"]["Default"] == "true"
    assert parsed["Parameters"]["NumberString"]["Default"] == "123"
    assert parsed["Parameters"]["YesString"]["Default"] == "yes"


def test_fn_join_converted_to_sub():
    """Fn::Join nodes should become Fn::Sub strings when safe."""

    input_yaml = """
Outputs:
  LambdaArn:
    Value:
      Fn::Join:
        - ':'
        - - arn
          - aws
          - lambda
          - !Ref AWS::Region
          - function
          - !GetAtt Handler.Arn
"""

    result = format_cfn_yaml(input_yaml, alignment_column=40)
    assert "!Sub" in result

    yaml = create_cfn_yaml()
    parsed = yaml.load(result)
    value = parsed["Outputs"]["LambdaArn"]["Value"]
    assert isinstance(value, CFNTag)
    assert value.tag == "Sub"
    assert "${AWS::Region}" in value.value
    assert "${Handler.Arn}" in value.value


def test_keys_with_colons_remain_valid():
    """Keys like aws:SourceIp must not be broken by alignment."""
    input_yaml = """
Resources:
  Domain:
    Type: AWS::OpenSearchService::Domain
    Properties:
      AccessPolicies:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              AWS: '*'
            Action: es:*
            Resource: arn:aws:es:us-east-1:123456789012:domain/test/*
            Condition:
              IpAddress:
                aws:SourceIp: 1.2.3.4/32
"""

    formatted = format_cfn_yaml(input_yaml, alignment_column=40)

    # Ensure the critical key is still present and the YAML parses
    assert "aws:SourceIp:" in formatted

    import yaml

    parsed = yaml.safe_load(formatted)
    cond = parsed["Resources"]["Domain"]["Properties"]["AccessPolicies"]["Statement"][0][
        "Condition"
    ]
    assert "IpAddress" in cond
    assert "aws:SourceIp" in cond["IpAddress"]


def test_header_uses_cdk_path_when_available():
    input_yaml = """
Resources:
  MyBucket:
    Type: AWS::S3::Bucket
    Metadata:
      aws:cdk:path: MyStack/NiceBucket/Resource
"""

    result = format_cfn_yaml(input_yaml)
    assert "## MyStack / NiceBucket / Resource" in result


def test_lowercase_resource_id_still_gets_header():
    input_yaml = """
Resources:
  mybucket:
    Type: AWS::S3::Bucket
"""

    result = format_cfn_yaml(input_yaml)
    assert "## mybucket" in result


def test_resource_titles_parameter_overrides_display():
    input_yaml = """
Resources:
  mybucket:
    Type: AWS::S3::Bucket
"""

    result = format_cfn_yaml(
        input_yaml,
        resource_titles={"mybucket": "MyStack / FancyBucket / Resource"},
    )
    assert "## MyStack / FancyBucket / Resource" in result


def test_compact_flow_style_collapses_small_maps_and_lists():
    input_yaml = """
Transform: AWS::Serverless-2016-10-31
Resources:
  Api:
    Type: AWS::Serverless::GraphQLApi
    Properties:
      Auth:
        Type: API_KEY
      RuntimeConfig:
        Name: APPSYNC_JS
        Version: 1.0.0
      Pipeline:
        - GetTaskFunction
Outputs:
  GraphQLAPIURL:
    Value: !GetAtt Api.GraphQLUrl
"""

    formatted = format_cfn_yaml(
        input_yaml,
        alignment_column=40,
        flow_style="compact",
    )
    lines = formatted.splitlines()

    auth_line = next(line for line in lines if "Auth:" in line)
    assert "{" in auth_line and "Type: API_KEY" in auth_line

    runtime_line = next(line for line in lines if "RuntimeConfig:" in line)
    assert (
        "{" in runtime_line
        and "Name: APPSYNC_JS" in runtime_line
        and "Version: 1.0.0" in runtime_line
    )

    pipeline_line = next(line for line in lines if "Pipeline:" in line)
    assert "[" in pipeline_line and "GetTaskFunction" in pipeline_line

    output_line = next(line for line in lines if "GraphQLAPIURL:" in line)
    # Accept with or without quotes on the GetAtt value
    assert (
        "{" in output_line and "Value: !GetAtt" in output_line and "Api.GraphQLUrl" in output_line
    )


def test_compact_flow_style_for_json_input():
    input_json = """
{
  "Transform": "AWS::Serverless-2016-10-31",
  "Resources": {
    "Api": {
      "Type": "AWS::Serverless::GraphQLApi",
      "Properties": {
        "Auth": {
          "Type": "API_KEY"
        },
        "Pipeline": ["GetTaskFunction"]
      }
    }
  }
}
"""

    formatted = format_cfn_yaml(
        input_json,
        alignment_column=40,
        flow_style="compact",
    )
    lines = formatted.splitlines()

    auth_line = next(line for line in lines if "Auth:" in line)
    assert "{" in auth_line and "Type: API_KEY" in auth_line

    pipeline_line = next(line for line in lines if "Pipeline:" in line)
    assert "[" in pipeline_line and "GetTaskFunction" in pipeline_line


def test_block_scalar_body_respects_structural_indent_when_column_small():
    """Block scalar bodies should still be indented as children of their key when column is small."""

    input_yaml = """
Resources:
  Api:
    Type: AWS::Serverless::GraphQLApi
    Properties:
      SchemaInline: |-
        type Query {
          getTask(id: ID!): Task
        }
"""

    formatted = format_cfn_yaml(
        input_yaml,
        alignment_column=1,
    )
    lines = formatted.splitlines()
    schema_line_index = next(idx for idx, line in enumerate(lines) if "SchemaInline:" in line)
    schema_line = lines[schema_line_index]
    base_indent = len(schema_line) - len(schema_line.lstrip(" "))

    # The first non-empty body line must be indented strictly more than the key,
    # i.e. as a structural child under SchemaInline.
    for body_line in lines[schema_line_index + 1 :]:
        if not body_line.strip():
            continue
        body_indent = len(body_line) - len(body_line.lstrip(" "))
        assert body_indent >= base_indent + 2
        assert "type Query" in body_line
        break


def test_conditions_nested_intrinsics_use_two_space_child_indent():
    """Nested intrinsic condition lists should use a 2-space child indent under parent dash."""

    input_yaml = """
Conditions:
  IsProduction: !Equals
    - !Ref Environment
    - prod
  IsNotDev: !Not
    - !Equals
      - !Ref Environment
      - dev
"""

    formatted = format_cfn_yaml(
        input_yaml,
        alignment_column=40,
    )
    lines = formatted.splitlines()

    # Locate the !Equals line and its child list items
    equals_index = next(i for i, line in enumerate(lines) if "IsNotDev:" in line)
    # The two list items under the inner !Equals should be indented 2 spaces
    # more than the inner dash line and have their values aligned to column 40.
    inner_dash_line = next(
        line
        for line in lines[equals_index + 1 :]
        if line.strip().startswith("-") and "!Equals" in line
    )
    inner_dash_indent = len(inner_dash_line) - len(inner_dash_line.lstrip(" "))

    ref_line = next(
        line
        for line in lines[equals_index + 1 :]
        if line.strip().startswith("-") and "!Ref Environment" in line
    )
    dev_line = next(
        line for line in lines[equals_index + 1 :] if line.strip().startswith("-") and "dev" in line
    )

    ref_indent = len(ref_line) - len(ref_line.lstrip(" "))
    dev_indent = len(dev_line) - len(dev_line.lstrip(" "))

    assert ref_indent == inner_dash_indent + 2
    assert dev_indent == inner_dash_indent + 2
    assert ref_line.index("!Ref Environment") >= 40
    assert dev_line.index("dev") >= 40


def test_existing_resource_comment_prevents_header_insertion():
    """If a resource already has a preceding comment block, do not add another header."""

    input_yaml = """
Resources:

  # Existing custom header for TasksTable
  # AppsyncDynamodbStack / TasksTable / Resource

  TasksTable:
    Type:                               AWS::DynamoDB::Table
    Properties:
      TableName:                        tasks
"""

    formatted = format_cfn_yaml(
        input_yaml,
        alignment_column=40,
    )

    # Preserve the existing comment lines
    assert "Existing custom header for TasksTable" in formatted
    assert "AppsyncDynamodbStack / TasksTable / Resource" in formatted

    # But do not introduce an additional Pretty CFN header for TasksTable itself
    assert "## TasksTable" not in formatted


def test_top_level_and_inline_comments_preserved_by_format():
    """Basic comments should round-trip through format_cfn_yaml."""

    input_yaml = """
# Template header comment
Resources:
  MyBucket:
    Type: AWS::S3::Bucket  # bucket inline comment
"""

    formatted = format_cfn_yaml(
        input_yaml,
        alignment_column=40,
    )

    assert "# Template header comment" in formatted
    assert "# bucket inline comment" in formatted


def test_resource_header_after_list_property_preserved():
    """Header comments between resources are preserved when previous resource ends with a list.

    This is a regression test for a bug where comments stored on list items (ca.items)
    were being lost during template processing. ruamel.yaml stores "before" comments
    for a following key on the last element of the preceding list.
    """

    input_yaml = """
Resources:

  ##
  ## First Resource Header
  ##

  FirstResource:
    Type: AWS::IAM::Policy
    Properties:
      PolicyName: TestPolicy
      Roles:
        - !Ref SomeRole

  ##
  ## Second Resource Header
  ##

  SecondResource:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: my-bucket
"""

    # Test with default block style
    formatted = format_cfn_yaml(input_yaml, alignment_column=40)

    # Both headers should be preserved
    assert "## First Resource Header" in formatted
    assert "## Second Resource Header" in formatted

    # The second header should NOT be replaced with just the resource name
    assert "## SecondResource" not in formatted

    # Test with compact flow style - should also preserve headers
    formatted_compact = format_cfn_yaml(input_yaml, alignment_column=40, flow_style="compact")

    assert "## First Resource Header" in formatted_compact
    assert "## Second Resource Header" in formatted_compact
    assert "## SecondResource" not in formatted_compact

    # The Roles list should NOT be converted to flow style since it has comments attached
    assert "Roles:" in formatted_compact
    assert (
        "- !Ref SomeRole" in formatted_compact
        or "-                               !Ref SomeRole" in formatted_compact
    )
