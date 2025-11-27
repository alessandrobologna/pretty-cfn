"""Deterministic helpers for refactor-oriented workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import shutil
import sys
import tempfile
from typing import Dict, Optional
import zipfile

from ..formatter import CFNTag
from ..samifier import AwsEnvironment, SamAssetRecord
from ..samifier.shared import (
    _detect_aws_env as _shared_detect_aws_env,
    _download_s3_object as _shared_download_s3_object,
    _extract_sub_parts as _shared_extract_sub_parts,
    _format_s3_uri as _shared_format_s3_uri,
    _infer_partition as _shared_infer_partition,
)
from ..service import (
    TemplateProcessingOptions,
    TemplateProcessingResult,
    TemplateSource,
    process_template,
)


@dataclass
class RefactorRequest:
    """Inputs for the refactor workflow."""

    stack_name: Optional[str] = None
    options: TemplateProcessingOptions = field(default_factory=TemplateProcessingOptions)
    output_path: Optional[Path] = None
    path: Optional[Path] = None


@dataclass
class RefactorArtifacts:
    """Artifacts produced by the deterministic refactor stage."""

    result: TemplateProcessingResult
    written_path: Optional[Path]
    asset_writes: list[tuple[Path, str, str]] = field(default_factory=list)

    @property
    def formatted_template(self) -> str:
        return self.result.formatted_content

    @property
    def rename_map(self) -> Dict[str, str]:
        return self.result.rename_map

    @property
    def sam_assets(self) -> list[SamAssetRecord]:
        return self.result.sam_assets


def run_refactor_stage(request: RefactorRequest) -> RefactorArtifacts:
    """Fetch and process the requested stack, returning artifacts + desired output path."""

    source_label = (
        f"stack='{request.stack_name}'" if request.stack_name else f"path='{request.path}'"
    )
    _log_refactor(f"starting refactor for {source_label}")
    options = _apply_refactor_defaults(request.options)
    if request.output_path:
        options.samify_relative_base = request.output_path
        _log_refactor(f"SAM project root requested: {request.output_path}")

    _log_refactor(
        "options: cdk_clean=%s cdk_samify=%s cdk_rename=%s samify_relative_base=%s"
        % (
            options.cdk_clean,
            options.cdk_samify,
            options.cdk_rename,
            options.samify_relative_base or "<auto>",
        )
    )
    if request.stack_name:
        source = TemplateSource(stack_name=request.stack_name)
    elif request.path:
        source = TemplateSource(path=request.path)
    else:
        raise ValueError("RefactorRequest requires either stack_name or path")

    stager: Optional[PlanningSamAssetStager] = None
    if request.output_path:
        stager = PlanningSamAssetStager(request.output_path)
    _log_refactor("fetching stack template via process_template()")
    template_result = process_template(source, options, sam_asset_stager=stager)
    _log_refactor(
        "process_template finished: lint_errors=%s lint_warnings=%s rename_map=%s sam_assets=%s"
        % (
            len(template_result.lint_errors),
            len(template_result.lint_warnings),
            len(template_result.rename_map),
            len(template_result.sam_assets),
        )
    )
    if request.output_path:
        _log_refactor(f"write plan ready for SAM project root {request.output_path}")
        if not request.output_path.exists():
            request.output_path.mkdir(parents=True, exist_ok=True)

        # Write the template
        template_path = request.output_path / "template.yaml"
        _write_text(template_path, template_result.formatted_content)

        # Write assets if stager was used
        if stager:
            stager.write_assets()
    else:
        _log_refactor("in-memory preview (no output path provided)")

    asset_writes: list[tuple[Path, str, str]] = []
    # We don't need to return asset_writes anymore if we are writing directly,
    # but keeping the structure for now to avoid breaking changes if needed.
    # In direct write mode, this list will be empty or we can populate it for reporting.

    return RefactorArtifacts(
        result=template_result,
        written_path=request.output_path,
        asset_writes=asset_writes,
    )


def _write_text(path: Path, content: str) -> None:
    text = content if content.endswith("\n") else content + "\n"
    path.write_text(text)


def _apply_refactor_defaults(options: TemplateProcessingOptions) -> TemplateProcessingOptions:
    """Ensure cdk_clean + samify are enabled for refactor flows."""

    opts = TemplateProcessingOptions(**vars(options))
    opts.cdk_clean = True
    opts.cdk_samify = True
    if opts.cdk_rename is None:
        opts.cdk_rename = True
    return opts


def _log_refactor(message: str) -> None:
    sys.stderr.write(f"[pretty-cfn][refactor] {message}\n")
    sys.stderr.flush()


class PlanningSamAssetStager:
    """Asset stager for MCP refactor flows that can write directly to disk.

    This mirrors the interface expected by the samifier helpers.
    It records desired file contents and can write them to disk when requested.
    """

    def __init__(self, project_root: Path, assets_subdir: str = "src") -> None:
        self.project_root = project_root
        self.asset_dir = project_root / assets_subdir
        self.records: list[SamAssetRecord] = []
        self._writes: dict[Path, str] = {}
        self._cache: dict[Path, Path] = {}
        self._aws_env: Optional[AwsEnvironment] = None

    # Public API used by samifier helpers ---------------------------------

    def stage_local_path(self, logical_id: str, source_path: Path) -> Path:
        resolved = source_path.resolve()
        staged = self._cache.get(resolved)
        if staged is None:
            staged = self._allocate_destination(resolved.name or "asset")
            if resolved.is_dir():
                for path in resolved.rglob("*"):
                    if not path.is_file():
                        continue
                    rel = path.relative_to(resolved)
                    dest = staged / rel
                    self._record_file(dest, path.read_text(encoding="utf-8", errors="replace"))
            else:
                self._record_file(staged, resolved.read_text(encoding="utf-8", errors="replace"))
            self._cache[resolved] = staged

        record = SamAssetRecord(logical_id=logical_id, source_path=resolved, staged_path=staged)
        self.records.append(record)
        return staged

    def stage_file_asset(
        self,
        logical_id: str,
        source_path: Path,
        *,
        file_name: Optional[str] = None,
    ) -> Path:
        resolved = source_path.resolve()
        target_dir = self._allocate_directory(logical_id)
        filename = file_name or resolved.name or "asset"
        dest = target_dir / filename
        self._record_file(dest, resolved.read_text(encoding="utf-8", errors="replace"))
        record = SamAssetRecord(logical_id=logical_id, source_path=resolved, staged_path=dest)
        self.records.append(record)
        return dest

    def stage_inline_text(
        self,
        logical_id: str,
        contents: str,
        *,
        file_name: str,
        ensure_trailing_newline: bool = True,
    ) -> Path:
        payload = (
            contents
            if (not ensure_trailing_newline or contents.endswith("\n"))
            else f"{contents}\n"
        )
        target_dir = self._allocate_directory(logical_id)
        dest = target_dir / file_name
        self._record_file(dest, payload)
        record = SamAssetRecord(
            logical_id=logical_id,
            source_path=Path(f"<inline:{logical_id}>>"),
            staged_path=target_dir,
        )
        self.records.append(record)
        return dest

    def stage_s3_code(
        self,
        logical_id: str,
        bucket: str,
        key: str,
        version: Optional[str] = None,
    ) -> Path:
        """Download and plan a zipped Lambda asset from S3.

        This mirrors SamAssetStager.stage_s3_code but records file contents
        into the write plan instead of touching disk under project_root.
        """

        temp_dir = Path(tempfile.mkdtemp(prefix="pretty-cfn-sam-plan-"))
        archive_path = temp_dir / "artifact.zip"
        try:
            _download_s3_object(bucket, key, version, archive_path)
            target_dir = self._allocate_directory(logical_id)
            with zipfile.ZipFile(archive_path) as zip_file:
                for info in zip_file.infolist():
                    if info.is_dir():
                        continue
                    target_file = target_dir / info.filename
                    data = zip_file.read(info)
                    text = data.decode("utf-8", errors="replace")
                    self._record_file(target_file, text)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

        source_uri = _format_s3_uri(bucket, key, version)
        record = SamAssetRecord(
            logical_id=logical_id,
            source_path=Path(source_uri),
            staged_path=target_dir,
        )
        self.records.append(record)
        return target_dir

    def stage_s3_file(
        self,
        logical_id: str,
        bucket: str,
        key: str,
        version: Optional[str] = None,
        *,
        file_name: Optional[str] = None,
    ) -> Path:
        """Download and plan a single file from S3."""

        temp_dir = Path(tempfile.mkdtemp(prefix="pretty-cfn-sam-plan-"))
        download_name = file_name or Path(key).name or "asset"
        download_path = temp_dir / download_name
        try:
            _download_s3_object(bucket, key, version, download_path)
            target_dir = self._allocate_directory(logical_id)
            dest = target_dir / download_name
            text = download_path.read_text(encoding="utf-8", errors="replace")
            self._record_file(dest, text)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

        source_uri = _format_s3_uri(bucket, key, version)
        record = SamAssetRecord(
            logical_id=logical_id,
            source_path=Path(source_uri),
            staged_path=dest,
        )
        self.records.append(record)
        return dest

    def apply_rename_map(self, rename_map: Dict[str, str]) -> None:
        if not rename_map:
            return
        new_writes: dict[Path, str] = {}
        for record in self.records:
            new_id = rename_map.get(record.logical_id)
            if not new_id or new_id == record.logical_id:
                continue
            old_root = record.staged_path
            new_root = old_root.parent / new_id
            for path, content in list(self._writes.items()):
                try:
                    rel = path.relative_to(old_root)
                except ValueError:
                    continue
                new_path = new_root / rel
                new_writes[new_path] = content
                del self._writes[path]
            record.logical_id = new_id
            record.staged_path = new_root
        self._writes.update(new_writes)

    def resolve_string(self, value) -> Optional[str]:
        """Resolve simple intrinsic strings, including Fn::Sub with AWS env.

        This mirrors SamAssetStager.resolve_string but avoids any filesystem
        writes. It supports direct strings and CFNTag(\"Sub\", ...) values used
        by the samifier helpers for S3 URI resolution.
        """

        if isinstance(value, str):
            return value
        if isinstance(value, CFNTag) and value.tag == "Sub":
            template, mapping = _extract_sub_parts(value.value)
            if template is None:
                return None
            env = self._ensure_aws_env()
            if env is None:
                return None
            replacements: Dict[str, str] = {
                "AWS::AccountId": env.account_id,
                "AWS::Region": env.region,
                "AWS::Partition": env.partition,
            }
            for key_name, raw in (mapping or {}).items():
                resolved = self.resolve_string(raw)
                if resolved is None:
                    return None
                replacements[key_name] = resolved
            result = template
            for key_name, replacement in replacements.items():
                result = result.replace(f"${{{key_name}}}", replacement)
            if "${" in result:
                return None
            return result
        return None

    def _ensure_aws_env(self) -> Optional[AwsEnvironment]:
        if self._aws_env is None:
            self._aws_env = _detect_aws_env()
        return self._aws_env

    # Planning helpers ----------------------------------------------------

    def build_write_plan(self) -> list[tuple[Path, str, str]]:
        """Return [(path, content, mode)] entries for all staged assets.

        Assets are always created under the project root; callers typically use
        `mode="create"` to avoid clobbering existing content.
        """

        entries: list[tuple[Path, str, str]] = []
        for path, content in sorted(self._writes.items(), key=lambda item: str(item[0])):
            entries.append((path, content, "create"))
        return entries

    def write_assets(self) -> None:
        """Write all staged assets to disk."""
        if not self.asset_dir.exists() and self._writes:
            self.asset_dir.mkdir(parents=True, exist_ok=True)

        for path, content in self._writes.items():
            if not path.parent.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
            _write_text(path, content)

    # Internal helpers ----------------------------------------------------

    def _record_file(self, dest: Path, text: str) -> None:
        self._writes[dest] = text

    def _allocate_destination(self, base_name: str) -> Path:
        sanitized = base_name or "asset"
        candidate = self.asset_dir / sanitized
        counter = 2
        while any(
            self._is_under(candidate, existing) or self._is_under(existing, candidate)
            for existing in self._writes
        ):
            candidate = self.asset_dir / f"{sanitized}-{counter}"
            counter += 1
        return candidate

    def _allocate_directory(self, logical_id: str) -> Path:
        target = self.asset_dir / logical_id
        # Drop any previous planned files under this logical id
        for path in list(self._writes.keys()):
            if self._is_under(path, target):
                del self._writes[path]
        return target

    @staticmethod
    def _is_under(path: Path, root: Path) -> bool:
        try:
            path.relative_to(root)
            return True
        except ValueError:
            return False


def _extract_sub_parts(value) -> tuple[Optional[str], Optional[Dict[str, object]]]:
    return _shared_extract_sub_parts(value)


def _download_s3_object(bucket: str, key: str, version: Optional[str], target: Path) -> None:
    return _shared_download_s3_object(bucket, key, version, target)


def _format_s3_uri(bucket: str, key: str, version: Optional[str]) -> str:
    return _shared_format_s3_uri(bucket, key, version)


def _detect_aws_env() -> Optional[AwsEnvironment]:
    env = _shared_detect_aws_env()
    return env


def _infer_partition(region: Optional[str]) -> str:
    return _shared_infer_partition(region)
