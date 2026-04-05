"""
Tests for result type __repr__ methods.

Result objects are constructed internally by the library; we verify they
serialize to meaningful strings for debugging.
"""

from __future__ import annotations

from iceberg_compaction import (
    CompactionResult,
    RewriteManifestsResult,
    ExpireSnapshotsResult,
    RemoveOrphanFilesResult,
)


class TestCompactionResultRepr:
    def test_repr_with_manifest_rewrite(self) -> None:
        # Build via the internal constructor — not exposed in public API,
        # but useful for verifying __repr__ format
        result = CompactionResult(
            input_files_count=10,
            output_files_count=3,
            input_bytes_total=1_000_000,
            output_bytes_total=900_000,
            manifest_rewrite=RewriteManifestsResult(
                kept_manifests_count=2,
                created_manifests_count=1,
                replaced_manifests_count=3,
                processed_entry_count=100,
            ),
        )
        r = repr(result)
        assert "CompactionResult" in r
        assert "10" in r
        assert "3" in r

    def test_repr_without_manifest_rewrite(self) -> None:
        result = CompactionResult(
            input_files_count=0,
            output_files_count=0,
            input_bytes_total=0,
            output_bytes_total=0,
            manifest_rewrite=None,
        )
        r = repr(result)
        assert "CompactionResult" in r


class TestRewriteManifestsResultRepr:
    def test_repr(self) -> None:
        result = RewriteManifestsResult(
            kept_manifests_count=5,
            created_manifests_count=2,
            replaced_manifests_count=3,
            processed_entry_count=1000,
        )
        r = repr(result)
        assert "RewriteManifestsResult" in r
        assert "kept=" in r
        assert "created=" in r
        assert "replaced=" in r
        assert "entries=" in r


class TestExpireSnapshotsResultRepr:
    def test_repr(self) -> None:
        result = ExpireSnapshotsResult(expired_snapshot_count=42)
        r = repr(result)
        assert "ExpireSnapshotsResult" in r
        assert "42" in r


class TestRemoveOrphanFilesResultRepr:
    def test_repr(self) -> None:
        result = RemoveOrphanFilesResult(
            orphan_file_paths=["s3://bucket/a.parquet", "s3://bucket/b.parquet"],
            orphan_files_found=2,
            orphan_files_deleted=2,
        )
        r = repr(result)
        assert "RemoveOrphanFilesResult" in r
        assert "found=" in r
        assert "deleted=" in r

    def test_repr_dry_run(self) -> None:
        result = RemoveOrphanFilesResult(
            orphan_file_paths=["s3://bucket/orphan.parquet"],
            orphan_files_found=1,
            orphan_files_deleted=0,  # dry_run=True
        )
        r = repr(result)
        assert "found=1" in r
        assert "deleted=0" in r
