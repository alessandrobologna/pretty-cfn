"""Asset staging helpers for SAM refactors."""

from __future__ import annotations

import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Optional
import zipfile

from ..formatter import CFNTag
from .shared import (
    _detect_aws_env as _shared_detect_aws_env,
    _download_s3_object as _download_s3_object,
    _extract_sub_parts as _shared_extract_sub_parts,
    _format_s3_uri as _shared_format_s3_uri,
    _infer_partition as _shared_infer_partition,
)


@dataclass
class SamAssetRecord:
    """Describe an asset that was staged into a SAM project."""

    logical_id: str
    source_path: Path
    staged_path: Path


@dataclass
class AwsEnvironment:
    account_id: str
    region: str
    partition: str


class SamAssetStager:
    """Copy Lambda assets into a target directory and keep a manifest."""

    def __init__(
        self,
        project_dir: Path,
        assets_subdir: str = "src",
        *,
        s3_downloader: Optional[Callable[[str, str, Optional[str], Path], None]] = None,
        aws_env: Optional[AwsEnvironment] = None,
    ) -> None:
        self.project_dir = project_dir
        self.asset_dir = project_dir / assets_subdir
        self.asset_dir.mkdir(parents=True, exist_ok=True)
        self.records: list[SamAssetRecord] = []
        self._cache: Dict[Path, Path] = {}
        self._s3_downloader = s3_downloader or _download_s3_object
        self._aws_env = aws_env

    def stage_local_path(self, logical_id: str, source_path: Path) -> Path:
        """Copy the given path into the assets directory, caching duplicates."""

        resolved = source_path.resolve()
        staged = self._cache.get(resolved)
        if staged is None:
            staged = self._allocate_destination(resolved.name or "asset")
            self._copy(resolved, staged)
            self._cache[resolved] = staged

        record = SamAssetRecord(logical_id=logical_id, source_path=resolved, staged_path=staged)
        self.records.append(record)
        return staged

    def stage_s3_code(
        self,
        logical_id: str,
        bucket: str,
        key: str,
        version: Optional[str] = None,
    ) -> Path:
        """Download an S3 Lambda asset (zip) and extract it under src/logical_id."""

        target_dir = self._allocate_directory(logical_id)
        temp_dir = Path(tempfile.mkdtemp(prefix="pretty-cfn-sam-"))
        artifact_path = temp_dir / "artifact.zip"
        try:
            self._s3_downloader(bucket, key, version, artifact_path)
            _extract_zip(artifact_path, target_dir)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

        record = SamAssetRecord(
            logical_id=logical_id,
            source_path=Path(_format_s3_uri(bucket, key, version)),
            staged_path=target_dir,
        )
        self.records.append(record)
        return target_dir

    def stage_file_asset(
        self,
        logical_id: str,
        source_path: Path,
        *,
        file_name: Optional[str] = None,
    ) -> Path:
        """Copy a single file into src/logical_id/<file>."""

        resolved = source_path.resolve()
        target_dir = self._allocate_directory(logical_id)
        filename = file_name or resolved.name or "asset"
        target = target_dir / filename
        shutil.copy2(resolved, target)

        record = SamAssetRecord(logical_id=logical_id, source_path=resolved, staged_path=target)
        self.records.append(record)
        return target

    def stage_inline_text(
        self,
        logical_id: str,
        contents: str,
        *,
        file_name: str,
        ensure_trailing_newline: bool = True,
    ) -> Path:
        """Materialize inline text into src/logical_id/<file_name>."""

        payload = (
            contents
            if (contents.endswith("\n") or not ensure_trailing_newline)
            else f"{contents}\n"
        )
        target_dir = self._allocate_directory(logical_id)
        target_file = target_dir / file_name
        target_file.parent.mkdir(parents=True, exist_ok=True)
        target_file.write_text(payload)
        record = SamAssetRecord(
            logical_id=logical_id,
            source_path=Path(f"<inline:{logical_id}>"),
            staged_path=target_dir,
        )
        self.records.append(record)
        return target_file

    def stage_s3_file(
        self,
        logical_id: str,
        bucket: str,
        key: str,
        version: Optional[str] = None,
        *,
        file_name: Optional[str] = None,
    ) -> Path:
        """Download a single file from S3 and place it under src/logical_id/."""

        temp_dir = Path(tempfile.mkdtemp(prefix="pretty-cfn-sam-"))
        download_name = file_name or Path(key).name or "asset"
        download_path = temp_dir / download_name
        try:
            self._s3_downloader(bucket, key, version, download_path)
            return self.stage_file_asset(logical_id, download_path, file_name=download_name)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def apply_rename_map(self, rename_map: Dict[str, str]) -> None:
        if not rename_map:
            return
        for record in self.records:
            new_id = rename_map.get(record.logical_id)
            if not new_id or new_id == record.logical_id:
                continue
            target = record.staged_path.parent / new_id
            if target.exists():
                shutil.rmtree(target)
            record.staged_path.rename(target)
            record.logical_id = new_id
            record.staged_path = target

    def resolve_string(self, value) -> Optional[str]:
        if isinstance(value, str):
            return value
        if isinstance(value, CFNTag) and value.tag == "Sub":
            template, mapping = _extract_sub_parts(value.value)
            if template is None:
                return None
            env = self._ensure_aws_env()
            if env is None:
                return None
            replacements = {
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

    def _allocate_destination(self, base_name: str) -> Path:
        sanitized = base_name or "asset"
        candidate = self.asset_dir / sanitized
        counter = 2
        while candidate.exists():
            candidate = self.asset_dir / f"{sanitized}-{counter}"
            counter += 1
        candidate.parent.mkdir(parents=True, exist_ok=True)
        return candidate

    def _allocate_directory(self, logical_id: str) -> Path:
        target = self.asset_dir / logical_id
        if target.exists():
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        target.mkdir(parents=True, exist_ok=True)
        return target

    def _copy(self, source: Path, dest: Path) -> None:
        if source.is_dir():
            shutil.copytree(source, dest)
        else:
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, dest)


def _extract_sub_parts(value) -> tuple[Optional[str], Optional[Dict[str, object]]]:
    return _shared_extract_sub_parts(value)


def _extract_zip(archive_path: Path, target_dir: Path) -> None:
    if not archive_path.exists():
        raise RuntimeError(f"Downloaded artifact not found at {archive_path}")
    with zipfile.ZipFile(archive_path) as zip_file:
        zip_file.extractall(target_dir)


def _format_s3_uri(bucket: str, key: str, version: Optional[str]) -> str:
    return _shared_format_s3_uri(bucket, key, version)


def _detect_aws_env() -> Optional[AwsEnvironment]:
    env = _shared_detect_aws_env()
    return env


def _infer_partition(region: Optional[str]) -> str:
    return _shared_infer_partition(region)
