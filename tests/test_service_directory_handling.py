import pytest
from pretty_cfn.service import TemplateSource, TemplateProcessingError


def test_load_directory_single_template_in_cdk_out(tmp_path):
    """Test finding a single template inside cdk.out subdir."""
    cdk_out = tmp_path / "cdk.out"
    cdk_out.mkdir()
    template = cdk_out / "MyStack.template.json"
    template.write_text('{"Resources": {}}')

    source = TemplateSource(path=tmp_path)
    content, name = source.load()

    assert content == '{"Resources": {}}'
    assert name == str(template)


def test_load_directory_is_cdk_out(tmp_path):
    """Test finding a template when path IS cdk.out."""
    template = tmp_path / "MyStack.template.json"
    template.write_text('{"Resources": {}}')

    source = TemplateSource(path=tmp_path)
    content, name = source.load()

    assert content == '{"Resources": {}}'
    assert name == str(template)


def test_load_directory_multiple_templates(tmp_path):
    """Test error when multiple templates exist."""
    (tmp_path / "Stack1.template.json").write_text("{}")
    (tmp_path / "Stack2.template.json").write_text("{}")

    source = TemplateSource(path=tmp_path)
    with pytest.raises(TemplateProcessingError) as exc:
        source.load()

    assert "Multiple templates found" in str(exc.value)
    assert "Stack1.template.json" in str(exc.value)
    assert "Stack2.template.json" in str(exc.value)


def test_load_directory_no_templates(tmp_path):
    """Test error when no templates exist."""
    source = TemplateSource(path=tmp_path)
    with pytest.raises(TemplateProcessingError) as exc:
        source.load()

    assert "No templates (*.template.json) found" in str(exc.value)


def test_load_file_still_works(tmp_path):
    """Ensure standard file loading still works."""
    template = tmp_path / "template.yaml"
    template.write_text("Resources: {}")

    source = TemplateSource(path=template)
    content, name = source.load()

    assert content == "Resources: {}"
    assert name == str(template)
