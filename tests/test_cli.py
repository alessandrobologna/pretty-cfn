"""CLI tests for pretty-cfn using Click's CliRunner."""

import json
from pathlib import Path

from click.testing import CliRunner

from pretty_cfn.cli import main, refactor_command, should_use_colors
from pretty_cfn.service import _discover_cdk_out


def test_format_stdin_to_stdout_formats():
    runner = CliRunner()
    input_yaml = "AWSTemplateFormatVersion: '2010-09-09'\nDescription: test\n"
    result = runner.invoke(main, [], input=input_yaml)
    assert result.exit_code == 0
    assert "Description:" in result.output


def test_format_with_output_writes_file(tmp_path: Path):
    runner = CliRunner()
    template = "Parameters:\n  P:\n    Type: String\n"
    out_file = tmp_path / "out.yaml"
    result = runner.invoke(main, ["-o", str(out_file)], input=template)
    assert result.exit_code == 0
    assert out_file.exists()
    assert "Parameters" in out_file.read_text()


def test_check_requires_input(tmp_path: Path):
    runner = CliRunner()
    result = runner.invoke(main, ["--check"], input="Description: hi\n")
    assert result.exit_code != 0
    assert "requires --input" in result.output


def test_diff_with_stdin_prints_headers():
    runner = CliRunner()
    template = "AWSTemplateFormatVersion: '2010-09-09'\nDescription: hi\n"

    res = runner.invoke(main, ["--diff"], input=template)
    assert res.exit_code == 0
    assert res.output.startswith("--- <stdin>\n+++ <stdout>\n")

    res2 = runner.invoke(main, ["--diff", "--diff-exit-code"], input=template)
    assert res2.exit_code == 1


def test_plain_flag_disables_colors(monkeypatch):
    runner = CliRunner()
    template = "Description: hi\n"

    monkeypatch.setattr(
        "pretty_cfn.cli.should_use_colors",
        lambda disable, output, tty: not disable,
    )
    monkeypatch.setattr(
        "pretty_cfn.cli.apply_syntax_highlighting",
        lambda text: f"COLOR::{text}",
    )

    colored = runner.invoke(main, [], input=template)
    assert colored.exit_code == 0
    assert "COLOR::" in colored.output

    plain = runner.invoke(main, ["--plain"], input=template)
    assert plain.exit_code == 0
    assert "COLOR::" not in plain.output


def test_should_use_colors_helper(tmp_path: Path):
    assert should_use_colors(False, None, True) is True
    assert should_use_colors(True, None, True) is False
    assert should_use_colors(False, tmp_path / "foo.yaml", True) is False


def test_lint_failure_skips_overwrite(monkeypatch, tmp_path: Path):
    from pretty_cfn.service import LintIssue

    def fake_lint(content: str, template_name: str):
        error = LintIssue(
            rule_id="E9999",
            message="Ref Foo is invalid",
            filename="template.yaml",
            line=3,
            column=5,
            severity="error",
        )
        return [], [error]

    monkeypatch.setattr("pretty_cfn.service.lint_template", fake_lint)

    runner = CliRunner()
    template_path = tmp_path / "template.yaml"
    template_path.write_text("Description: hi\n")

    result = runner.invoke(main, ["--input", str(template_path), "--overwrite"])
    assert result.exit_code == 1
    assert "--overwrite skipped" in result.output
    # File was not modified
    assert template_path.read_text() == "Description: hi\n"
    # Output still printed to stdout so the user can inspect it
    assert "Description:" in result.output


def test_ignore_errors_allows_success_exit(monkeypatch, tmp_path: Path):
    from pretty_cfn.service import LintIssue

    def fake_lint(content: str, template_name: str):
        error = LintIssue(
            rule_id="E9999",
            message="Ref Foo is invalid",
            filename="template.yaml",
            line=3,
            column=5,
            severity="error",
        )
        return [], [error]

    monkeypatch.setattr("pretty_cfn.service.lint_template", fake_lint)

    runner = CliRunner()
    template_path = tmp_path / "template.yaml"
    template_path.write_text("Description: hi\n")

    result = runner.invoke(
        main,
        ["--input", str(template_path), "--overwrite", "--ignore-errors"],
    )

    assert result.exit_code == 0
    assert "ignored" in result.output
    assert "Description:" in template_path.read_text()


def test_lint_warning_prints_when_flag(monkeypatch):
    from pretty_cfn.service import LintIssue

    def fake_lint(content: str, template_name: str):
        warning = LintIssue(
            rule_id="W1234",
            message="Optional warning",
            filename="template.yaml",
            line=1,
            column=1,
            severity="warning",
        )
        return [warning], []

    monkeypatch.setattr("pretty_cfn.service.lint_template", fake_lint)

    runner = CliRunner()
    result = runner.invoke(main, ["--lint"], input="Description: hi\n")
    assert result.exit_code == 0
    assert "Optional warning" in result.output


def test_lint_warning_suppressed_without_flag(monkeypatch):
    from pretty_cfn.service import LintIssue

    def fake_lint(content: str, template_name: str):
        warning = LintIssue(
            rule_id="W1234",
            message="Optional warning",
            filename="template.yaml",
            line=1,
            column=1,
            severity="warning",
        )
        return [warning], []

    monkeypatch.setattr("pretty_cfn.service.lint_template", fake_lint)

    runner = CliRunner()
    result = runner.invoke(main, [], input="Description: hi\n")
    assert result.exit_code == 0
    assert "Optional warning" not in result.output


def test_stack_name_download(monkeypatch):
    template_body = "Resources:\n  Bucket:\n    Type: AWS::S3::Bucket\n"
    # Patch where the function is used (in cli module), not where it's defined
    monkeypatch.setattr("pretty_cfn.cli._service_fetch_stack_template", lambda name: template_body)

    runner = CliRunner()
    result = runner.invoke(main, ["--stack-name", "MyStack"], input="")
    assert result.exit_code == 0
    assert "Bucket" in result.output


def test_stack_name_conflicts_with_input(tmp_path: Path):
    template = tmp_path / "template.yaml"
    template.write_text("Description: hi\n")

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["--input", str(template), "--stack-name", "MyStack"],
    )
    assert result.exit_code != 0
    assert "cannot be combined" in result.output


def test_empty_sections_removed():
    runner = CliRunner()
    template = "Resources:\n  Bucket:\n    Type: AWS::S3::Bucket\nConditions: {}\nOutputs: {}\n"

    result = runner.invoke(main, [], input=template)
    assert result.exit_code == 0
    assert "Conditions" not in result.output
    assert "Outputs" not in result.output


def test_check_detects_trailing_newline_difference(tmp_path: Path):
    runner = CliRunner()
    template = tmp_path / "template.yaml"
    template.write_text("Description:                            hi")

    result = runner.invoke(main, ["--check", "--input", str(template)])
    assert result.exit_code == 1
    assert "needs formatting" in result.output


def test_refactor_clean_cfn_normalizes_ids(tmp_path: Path):
    runner = CliRunner()
    template = tmp_path / "template.yaml"
    template.write_text("Resources:\n  MyBucketF68F3FF0:\n    Type: AWS::S3::Bucket\n")
    out_file = tmp_path / "out.yaml"

    result = runner.invoke(
        refactor_command,
        [
            "--target",
            "clean-cfn",
            "--input",
            str(template),
            "-o",
            str(out_file),
        ],
    )
    assert result.exit_code == 0
    rendered = out_file.read_text()
    assert "MyBucketF68F3FF0" not in rendered
    assert "MyBucket:" in rendered


def test_refactor_report_only_outputs_json(tmp_path: Path):
    runner = CliRunner()
    template = tmp_path / "template.yaml"
    template.write_text("Description: hi\n")
    report_file = tmp_path / "report.json"

    result = runner.invoke(
        refactor_command,
        [
            "--target",
            "report-only",
            "--input",
            str(template),
            "-o",
            str(report_file),
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(report_file.read_text())
    assert payload["source"].endswith("template.yaml")
    assert "traits" in payload


def test_refactor_sam_app_creates_project(tmp_path: Path, monkeypatch):
    import zipfile
    from pretty_cfn.samifier.asset_stager import AwsEnvironment

    # Mock AWS environment detection to avoid boto3 calls
    def mock_detect_aws_env():
        return AwsEnvironment(account_id="123456789012", region="us-east-1", partition="aws")

    monkeypatch.setattr("pretty_cfn.samifier.asset_stager._detect_aws_env", mock_detect_aws_env)

    # Mock S3 download to create empty zip without boto3
    def mock_download_s3_object(bucket, key, version, target):
        target.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(target, "w"):
            pass

    monkeypatch.setattr(
        "pretty_cfn.samifier.asset_stager._download_s3_object", mock_download_s3_object
    )

    runner = CliRunner()
    project = tmp_path / "cdk"
    asset_dir = project / "cdk.out" / "asset.xyz"
    asset_dir.mkdir(parents=True)
    (asset_dir / "index.js").write_text("exports.handler = async () => {};\n")

    template_path = project / "template.yaml"
    template_path.write_text(
        """Resources:\n  AssetFunction:\n    Type: AWS::Lambda::Function\n    Properties:\n      Code:\n        S3Bucket: Assets\n        S3Key: asset.zip\n      Handler: index.handler\n      Runtime: nodejs22.x\n    Metadata:\n      aws:asset:path: cdk.out/asset.xyz\n      aws:asset:property: Code\n"""
    )

    output_dir = tmp_path / "sam"
    result = runner.invoke(
        refactor_command,
        [
            "--target",
            "sam-app",
            "--input",
            str(template_path),
            "--output",
            str(output_dir),
            "--overwrite",
        ],
    )
    assert result.exit_code == 0
    rendered = (output_dir / "template.yaml").read_text()
    assert "AWS::Serverless::Function" in rendered
    assert "src/AssetFunction" in rendered
    staged = output_dir / "src" / "AssetFunction"
    assert staged.exists()


def test_refactor_sam_app_renames_asset_dirs(tmp_path: Path, monkeypatch):
    import zipfile
    from pretty_cfn.samifier.asset_stager import AwsEnvironment

    # Mock AWS environment detection to avoid boto3 calls
    def mock_detect_aws_env():
        return AwsEnvironment(account_id="123456789012", region="us-east-1", partition="aws")

    monkeypatch.setattr("pretty_cfn.samifier.asset_stager._detect_aws_env", mock_detect_aws_env)

    # Mock S3 download to create empty zip without boto3
    def mock_download_s3_object(bucket, key, version, target):
        target.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(target, "w"):
            pass

    monkeypatch.setattr(
        "pretty_cfn.samifier.asset_stager._download_s3_object", mock_download_s3_object
    )

    runner = CliRunner()
    asset_dir = tmp_path / "assets"
    asset_dir.mkdir()
    (asset_dir / "index.js").write_text("exports.handler = async () => {}\n")

    template = tmp_path / "template.yaml"
    template.write_text(
        """Resources:\n  MyFuncB2AB6E79:\n    Type: AWS::Lambda::Function\n    Properties:\n      Code:\n        S3Bucket: bucket\n        S3Key: code.zip\n      Handler: index.handler\n      Runtime: nodejs22.x\n    Metadata:\n      aws:asset:path: assets\n      aws:asset:property: Code\n"""
    )

    output_dir = tmp_path / "out"
    result = runner.invoke(
        refactor_command,
        [
            "--target",
            "sam-app",
            "--input",
            str(template),
            "--output",
            str(output_dir),
            "--overwrite",
        ],
    )

    assert result.exit_code == 0
    rendered = (output_dir / "template.yaml").read_text()
    assert "src/MyFunc" in rendered
    assert (output_dir / "src" / "MyFunc").exists()


def test_refactor_sam_app_converts_graphql(tmp_path: Path, monkeypatch):
    import zipfile
    from pretty_cfn.samifier.asset_stager import AwsEnvironment

    # Mock AWS environment detection to avoid boto3 calls
    def mock_detect_aws_env():
        return AwsEnvironment(account_id="123456789012", region="us-east-1", partition="aws")

    monkeypatch.setattr("pretty_cfn.samifier.asset_stager._detect_aws_env", mock_detect_aws_env)

    # Mock S3 download to create empty zip without boto3
    def mock_download_s3_object(bucket, key, version, target):
        target.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(target, "w"):
            pass

    monkeypatch.setattr(
        "pretty_cfn.samifier.asset_stager._download_s3_object", mock_download_s3_object
    )

    runner = CliRunner()
    project = tmp_path / "proj"
    project.mkdir()
    (project / "asset.cars.js").write_text("export const request = () => ({});\n")
    (project / "asset.resolver.js").write_text("export function request() { return {}; }\n")

    template = project / "template.yaml"
    template.write_text(
        """Resources:
  DataSourceRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: appsync.amazonaws.com
            Action: sts:AssumeRole
  CarTable:
    Type: AWS::DynamoDB::Table
    Properties:
      AttributeDefinitions:
        - AttributeName: id
          AttributeType: S
      KeySchema:
        - AttributeName: id
          KeyType: HASH
      BillingMode: PAY_PER_REQUEST
  CarApi:
    Type: AWS::AppSync::GraphQLApi
    Properties:
      AuthenticationType: AWS_IAM
      Name: cars
  GraphSchema:
    Type: AWS::AppSync::GraphQLSchema
    Properties:
      ApiId: !GetAtt CarApi.ApiId
      Definition: "schema {\\n  query: Query\\n}\\n\\ntype Query {\\n  getCar: String\\n}\\n"
  CarsDataSource:
    Type: AWS::AppSync::DataSource
    Properties:
      ApiId: !GetAtt CarApi.ApiId
      Name: CarsSource
      Type: AMAZON_DYNAMODB
      ServiceRoleArn: !GetAtt DataSourceRole.Arn
      DynamoDBConfig:
        AwsRegion: us-east-1
        TableName: !Ref CarTable
  CarsFunction:
    Type: AWS::AppSync::FunctionConfiguration
    Properties:
      ApiId: !GetAtt CarApi.ApiId
      Name: listCars
      DataSourceName: CarsSource
      Runtime:
        Name: APPSYNC_JS
        RuntimeVersion: '1.0.0'
      CodeS3Location: s3://bucket/cars.js
  CarsResolver:
    Type: AWS::AppSync::Resolver
    Properties:
      ApiId: !GetAtt CarApi.ApiId
      TypeName: Query
      FieldName: getCar
      Kind: PIPELINE
      Runtime:
        Name: APPSYNC_JS
        RuntimeVersion: '1.0.0'
      PipelineConfig:
        Functions:
          - !GetAtt CarsFunction.FunctionId
      CodeS3Location: s3://bucket/resolver.js
"""
    )

    output_dir = tmp_path / "sam"
    result = runner.invoke(
        refactor_command,
        [
            "--target",
            "sam-app",
            "--input",
            str(template),
            "--output",
            str(output_dir),
            "--overwrite",
        ],
    )
    assert result.exit_code == 0
    rendered = (output_dir / "template.yaml").read_text()
    assert "AWS::Serverless::GraphQLApi" in rendered
    schema_line = next(
        (line for line in rendered.splitlines() if "SchemaInline:" in line),
        "",
    )
    assert "|-" in schema_line
    assert "Resolvers:" in rendered
    assert "function.js" in rendered
    assert "resolver.js" in rendered
    assert (output_dir / "src" / "CarsFunction" / "function.js").exists()
    assert (output_dir / "src" / "CarsResolver" / "resolver.js").exists()


def test_refactor_sam_app_requires_output(tmp_path: Path):
    runner = CliRunner()
    template = tmp_path / "template.yaml"
    template.write_text("Description: hi\n")

    result = runner.invoke(
        refactor_command,
        [
            "--target",
            "sam-app",
            "--input",
            str(template),
        ],
    )
    assert result.exit_code != 0
    assert "--output is required" in result.output


def test_graphql_definition_rendered_as_block(tmp_path: Path):
    runner = CliRunner()
    template = tmp_path / "api.yaml"
    template.write_text(
        """Resources:\n  Schema:\n    Type: AWS::AppSync::GraphQLSchema\n    Properties:\n      ApiId: !Ref Api\n      Definition: "type Query {\\n  ping: String\\n}\\n"\n"""
    )

    result = runner.invoke(main, ["--input", str(template), "--output", str(template)])
    assert result.exit_code == 0
    rendered = template.read_text()
    assert "|-" in rendered
    assert "ping:" in rendered


def test_discover_cdk_out_from_template(tmp_path: Path):
    cdk_dir = tmp_path / "proj" / "cdk.out"
    cdk_dir.mkdir(parents=True)
    template = cdk_dir / "Stack.template.json"
    template.write_text("{}")

    detected = _discover_cdk_out(template)
    assert detected == cdk_dir


def test_discover_cdk_out_from_cwd(tmp_path: Path, monkeypatch):
    project = tmp_path / "proj"
    cdk_dir = project / "cdk.out"
    cdk_dir.mkdir(parents=True)
    monkeypatch.chdir(project)
    detected = _discover_cdk_out(None)
    assert detected == cdk_dir
