import zipfile


from pretty_cfn.agents.refactor_workflow import (
    PlanningSamAssetStager,
    RefactorRequest,
    run_refactor_stage,
)
from pretty_cfn.formatter import CFNTag
from pretty_cfn.samifier import AwsEnvironment
from pretty_cfn.service import TemplateProcessingResult


def _fake_result() -> TemplateProcessingResult:
    return TemplateProcessingResult(
        source_name="stack",
        original_content="orig",
        formatted_content="formatted",
    )


def test_run_refactor_stage_with_output(monkeypatch, tmp_path):
    captured = {}
    fake_result = _fake_result()

    def fake_process(source, options, sam_asset_stager=None):
        captured["stack_name"] = source.stack_name
        captured["options"] = options
        return fake_result

    monkeypatch.setattr(
        "pretty_cfn.agents.refactor_workflow.process_template",
        fake_process,
    )

    output_root = tmp_path / "out"
    request = RefactorRequest(stack_name="ProdStack", output_path=output_root)
    artifacts = run_refactor_stage(request)

    assert artifacts.result is fake_result
    assert artifacts.written_path == output_root
    assert captured["stack_name"] == "ProdStack"
    assert captured["options"].cdk_clean is True
    assert captured["options"].cdk_samify is True
    assert captured["options"].cdk_rename is True
    assert captured["options"].samify_relative_base == output_root


def test_run_refactor_stage_in_memory(monkeypatch):
    captured = {}
    fake_result = _fake_result()

    def fake_process(source, options, sam_asset_stager=None):
        captured["stack_name"] = source.stack_name
        captured["options"] = options
        return fake_result

    monkeypatch.setattr(
        "pretty_cfn.agents.refactor_workflow.process_template",
        fake_process,
    )

    request = RefactorRequest(stack_name="LiveStack")
    artifacts = run_refactor_stage(request)

    assert artifacts.result is fake_result
    assert artifacts.written_path is None
    assert captured["stack_name"] == "LiveStack"
    assert captured["options"].cdk_clean is True
    assert captured["options"].cdk_samify is True
    assert captured["options"].cdk_rename is True


def test_planning_stager_resolve_string_with_sub(monkeypatch, tmp_path):
    def fake_detect_env():
        return AwsEnvironment(account_id="123456789012", region="us-west-2", partition="aws")

    monkeypatch.setattr(
        "pretty_cfn.agents.refactor_workflow._detect_aws_env",
        fake_detect_env,
    )

    root = tmp_path / "sam-app"
    stager = PlanningSamAssetStager(root)
    template = "s3://${AWS::AccountId}-${AWS::Region}/${Name}"
    mapping = {"Name": "artifacts"}
    tag = CFNTag("Sub", [template, mapping])

    resolved = stager.resolve_string(tag)
    assert resolved == "s3://123456789012-us-west-2/artifacts"


def test_planning_stager_stage_s3_file(monkeypatch, tmp_path):
    calls = {}

    def fake_download(bucket, key, version, target):
        calls["args"] = (bucket, key, version)
        target.write_text("from handler import main\n", encoding="utf-8")

    monkeypatch.setattr(
        "pretty_cfn.agents.refactor_workflow._download_s3_object",
        fake_download,
    )

    root = tmp_path / "sam-app"
    stager = PlanningSamAssetStager(root)
    dest = stager.stage_s3_file(
        "MyFunction",
        "my-bucket",
        "code/artifact.py",
        version="v1",
        file_name="index.py",
    )

    assert dest == root / "src" / "MyFunction" / "index.py"
    writes = stager.build_write_plan()
    assert writes == [
        (root / "src" / "MyFunction" / "index.py", "from handler import main\n", "create"),
    ]
    assert calls["args"] == ("my-bucket", "code/artifact.py", "v1")


def test_planning_stager_stage_s3_code(monkeypatch, tmp_path):
    calls = {}

    def fake_download(bucket, key, version, target):
        calls["args"] = (bucket, key, version)
        with zipfile.ZipFile(target, "w") as zf:
            zf.writestr("handler/index.py", "def handler(event, ctx):\n    return {}\n")
            zf.writestr("README.md", "# sample\n")

    monkeypatch.setattr(
        "pretty_cfn.agents.refactor_workflow._download_s3_object",
        fake_download,
    )

    root = tmp_path / "sam-app"
    stager = PlanningSamAssetStager(root)
    staged_dir = stager.stage_s3_code(
        "MyFunction",
        "my-bucket",
        "code/function.zip",
        version=None,
    )

    assert staged_dir == root / "src" / "MyFunction"
    writes = sorted(stager.build_write_plan(), key=lambda item: str(item[0]))
    paths = [p for p, _, _ in writes]
    assert paths == [
        root / "src" / "MyFunction" / "README.md",
        root / "src" / "MyFunction" / "handler" / "index.py",
    ]
    contents = {str(p): c for p, c, _ in writes}
    assert contents[str(root / "src" / "MyFunction" / "handler" / "index.py")].startswith(
        "def handler"
    )
    assert "# sample" in contents[str(root / "src" / "MyFunction" / "README.md")]
