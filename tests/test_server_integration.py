"""Integration tests for MCP server tools with service layer."""

from pretty_cfn.server import _build_options
from pretty_cfn.service import (
    TemplateSource,
    TemplateProcessingOptions,
    process_template,
    process_file,
    lint_template,
)


SAMPLE_TEMPLATE = """
AWSTemplateFormatVersion: '2010-09-09'
Resources:
  MyBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: my-test-bucket
""".strip()


def test_build_options_basic():
    """Test that _build_options creates TemplateProcessingOptions correctly."""
    opts = _build_options(column=50, flow_style="compact")

    assert isinstance(opts, TemplateProcessingOptions)
    assert opts.column == 50
    assert opts.flow_style == "compact"


def test_build_options_ignores_none():
    """Test that _build_options ignores None values and keeps defaults."""
    opts = _build_options(column=50, flow_style=None)

    assert opts.column == 50
    # flow_style keeps its default value from TemplateProcessingOptions
    assert opts.flow_style == "block"


def test_build_options_empty():
    """Test that _build_options with no args returns defaults."""
    opts = _build_options()

    assert isinstance(opts, TemplateProcessingOptions)
    # Defaults from TemplateProcessingOptions
    assert opts.column == 40
    assert opts.flow_style == "block"


def test_format_with_options():
    """Test formatting with options."""
    opts = _build_options(column=45)
    source = TemplateSource(inline_content=SAMPLE_TEMPLATE)
    result = process_template(source, opts)

    assert result is not None
    assert "AWSTemplateFormatVersion" in result.formatted_content
    assert result.summary is not None


def test_lint_with_content():
    """Test linting with content string."""
    content = SAMPLE_TEMPLATE

    warnings, errors = lint_template(content, "<test>")

    assert isinstance(warnings, list)
    assert isinstance(errors, list)


def test_full_pipeline_with_cdk_options():
    """Test full formatting pipeline with CDK options."""
    cdk_template = """
AWSTemplateFormatVersion: '2010-09-09'
Resources:
  MyBucketABCD1234:
    Type: AWS::S3::Bucket
    Metadata:
      aws:cdk:path: MyStack/MyBucket/Resource
""".strip()

    opts = _build_options(cdk_clean=True, cdk_rename=True, cdk_keep_hashes=False)
    source = TemplateSource(inline_content=cdk_template)
    result = process_template(source, opts)

    assert result is not None
    assert result.cdk_cleaned is True


def test_format_inline_template():
    """Test formatting inline template content."""
    opts = _build_options(column=20)
    source = TemplateSource(inline_content=SAMPLE_TEMPLATE)
    result = process_template(source, opts)

    assert "my-test-bucket" in result.formatted_content
    assert result.source_name == "<stdin>"


def test_format_local_file(tmp_path):
    """Test formatting a local template file."""
    template_path = tmp_path / "template.yaml"
    template_path.write_text(SAMPLE_TEMPLATE)

    opts = _build_options(column=20)
    result = process_file(template_path, opts)

    assert "my-test-bucket" in result.formatted_content
    assert "Type:           AWS::S3::Bucket" in result.formatted_content


def test_format_local_file_with_output(tmp_path):
    """Test formatting and writing to output path."""
    template_path = tmp_path / "input.yaml"
    output_path = tmp_path / "output.yaml"
    template_path.write_text(SAMPLE_TEMPLATE)

    opts = _build_options(column=20)
    result = process_file(template_path, opts)

    # Write output
    output_path.write_text(result.formatted_content)

    # Verify output file was written
    assert output_path.exists()
    content = output_path.read_text()
    assert "my-test-bucket" in content
    assert "Type:           AWS::S3::Bucket" in content

    # Verify input file was NOT modified
    assert template_path.read_text() == SAMPLE_TEMPLATE
