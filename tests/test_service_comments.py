from __future__ import annotations

from pretty_cfn.service import TemplateProcessingOptions, TemplateSource, process_template
from pretty_cfn.samifier import SamAssetStager


def test_sam_app_refactor_preserves_resource_header_comment(tmp_path):
    """End-to-end SAM app refactor should keep resource header comments."""

    input_yaml = """
Resources:

  # Custom header for DemoFn
  DemoFn:
    Type: AWS::Lambda::Function
    Properties:
      Code:
        ZipFile: exports.handler = () => null
      Handler: index.handler
      Runtime: nodejs22.x
"""

    source = TemplateSource(path=None, stack_name=None, inline_content=input_yaml)
    project_dir = tmp_path / "sam-app"
    project_dir.mkdir()

    options = TemplateProcessingOptions(
        column=40,
        flow_style="block",
        cdk_clean=True,
        cdk_rename=True,
        cdk_semantic_naming=True,
        cdk_keep_path_metadata=True,
        cdk_collision_strategy="numbered",
        cdk_samify=True,
        samify_relative_base=project_dir,
        samify_prefer_external=False,
    )

    stager = SamAssetStager(project_dir, assets_subdir="src")
    result = process_template(source, options, sam_asset_stager=stager)

    formatted = result.formatted_content
    # The custom comment header should still be present in the final SAM template.
    assert "# Custom header for DemoFn" in formatted
    assert "AWS::Serverless::Function" in formatted


def test_clean_cfn_refactor_preserves_resource_header_comment(tmp_path):
    """CDK clean-only refactor should keep resource header comments."""

    input_yaml = """
Resources:

  # Custom header for Bucket
  MyBucketABCDEF12:
    Type: AWS::S3::Bucket
"""

    source = TemplateSource(path=None, stack_name=None, inline_content=input_yaml)

    options = TemplateProcessingOptions(
        column=40,
        flow_style="block",
        cdk_clean=True,
        cdk_rename=True,
        cdk_semantic_naming=True,
        cdk_keep_path_metadata=True,
        cdk_collision_strategy="numbered",
    )

    result = process_template(source, options)
    formatted = result.formatted_content

    # Header comment should survive cleaning and renaming of the resource key.
    assert "# Custom header for Bucket" in formatted
    # The resource should be renamed to a hash-stripped variant.
    assert "MyBucketABCDEF12" not in formatted
    assert "MyBucket" in formatted
