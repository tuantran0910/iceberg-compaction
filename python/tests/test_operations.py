"""
Tests for operation classes — construction, run(), and error propagation.
"""

from __future__ import annotations

import pytest

from iceberg_compaction import (
    Compaction,
    CompactionConfig,
    RewriteManifests,
    RewriteManifestsConfig,
    ExpireSnapshots,
    ExpireSnapshotsConfig,
    RemoveOrphanFiles,
    RemoveOrphanFilesConfig,
    RetryConfig,
    CatalogError,
)


class TestCompactionOperation:
    """Compaction operation construction and run."""

    def test_construction_with_defaults(
        self, memory_catalog: "MemoryCatalog"
    ) -> None:
        op = Compaction(
            catalog=memory_catalog,
            table="ns.tbl",
            config=CompactionConfig(),
        )
        assert op is not None

    def test_construction_with_retry_config(
        self, memory_catalog: "MemoryCatalog"
    ) -> None:
        op = Compaction(
            catalog=memory_catalog,
            table="ns.tbl",
            config=CompactionConfig(),
            to_branch="main",
            retry_config=RetryConfig(max_retries=5),
        )
        assert op is not None

    def test_repr(self, memory_catalog: "MemoryCatalog") -> None:
        op = Compaction(
            catalog=memory_catalog,
            table="ns.tbl",
            config=CompactionConfig(),
        )
        r = repr(op)
        assert "Compaction" in r

    def test_run_on_nonexistent_namespace_returns_error(
        self, memory_catalog: "MemoryCatalog"
    ) -> None:
        """Hitting a non-existent namespace propagates as CatalogError."""
        op = Compaction(
            catalog=memory_catalog,
            table="nonexistent_namespace.table",
            config=CompactionConfig(),
        )
        with pytest.raises(CatalogError):
            op.run()


class TestRewriteManifestsOperation:
    """RewriteManifests operation construction and run."""

    def test_construction_with_defaults(
        self, memory_catalog: "MemoryCatalog"
    ) -> None:
        op = RewriteManifests(
            catalog=memory_catalog,
            table="ns.tbl",
            config=RewriteManifestsConfig(),
        )
        assert op is not None

    def test_construction_with_retry_config(
        self, memory_catalog: "MemoryCatalog"
    ) -> None:
        op = RewriteManifests(
            catalog=memory_catalog,
            table="ns.tbl",
            config=RewriteManifestsConfig(),
            to_branch="main",
            retry_config=RetryConfig(),
        )
        assert op is not None

    def test_repr(self, memory_catalog: "MemoryCatalog") -> None:
        op = RewriteManifests(
            catalog=memory_catalog,
            table="ns.tbl",
            config=RewriteManifestsConfig(),
        )
        r = repr(op)
        assert "RewriteManifests" in r

    def test_run_on_nonexistent_namespace_returns_error(
        self, memory_catalog: "MemoryCatalog"
    ) -> None:
        """Hitting a non-existent namespace propagates as CatalogError."""
        op = RewriteManifests(
            catalog=memory_catalog,
            table="nonexistent_namespace.table",
            config=RewriteManifestsConfig(),
        )
        with pytest.raises(CatalogError):
            op.run()


class TestExpireSnapshotsOperation:
    """ExpireSnapshots operation construction and run."""

    def test_construction_with_defaults(
        self, memory_catalog: "MemoryCatalog"
    ) -> None:
        op = ExpireSnapshots(
            catalog=memory_catalog,
            table="ns.tbl",
            config=ExpireSnapshotsConfig(),
        )
        assert op is not None

    def test_construction_with_custom_config(
        self, memory_catalog: "MemoryCatalog"
    ) -> None:
        op = ExpireSnapshots(
            catalog=memory_catalog,
            table="ns.tbl",
            config=ExpireSnapshotsConfig(retain_last=10),
            to_branch="main",
            retry_config=RetryConfig(),
        )
        assert op is not None

    def test_repr(self, memory_catalog: "MemoryCatalog") -> None:
        op = ExpireSnapshots(
            catalog=memory_catalog,
            table="ns.tbl",
            config=ExpireSnapshotsConfig(),
        )
        r = repr(op)
        assert "ExpireSnapshots" in r

    def test_run_on_nonexistent_namespace_returns_error(
        self, memory_catalog: "MemoryCatalog"
    ) -> None:
        """Hitting a non-existent namespace propagates as CatalogError."""
        op = ExpireSnapshots(
            catalog=memory_catalog,
            table="nonexistent_namespace.table",
            config=ExpireSnapshotsConfig(),
        )
        with pytest.raises(CatalogError):
            op.run()


class TestRemoveOrphanFilesOperation:
    """RemoveOrphanFiles operation construction and run."""

    def test_construction_with_defaults(
        self, memory_catalog: "MemoryCatalog"
    ) -> None:
        # RemoveOrphanFiles does NOT accept retry_config or to_branch
        op = RemoveOrphanFiles(
            catalog=memory_catalog,
            table="ns.tbl",
            config=RemoveOrphanFilesConfig(),
        )
        assert op is not None

    def test_construction_with_dry_run(
        self, memory_catalog: "MemoryCatalog"
    ) -> None:
        op = RemoveOrphanFiles(
            catalog=memory_catalog,
            table="ns.tbl",
            config=RemoveOrphanFilesConfig(dry_run=True),
        )
        assert op is not None

    def test_repr(self, memory_catalog: "MemoryCatalog") -> None:
        op = RemoveOrphanFiles(
            catalog=memory_catalog,
            table="ns.tbl",
            config=RemoveOrphanFilesConfig(),
        )
        r = repr(op)
        assert "RemoveOrphanFiles" in r

    def test_run_on_nonexistent_namespace_returns_error(
        self, memory_catalog: "MemoryCatalog"
    ) -> None:
        """Hitting a non-existent namespace propagates as CatalogError."""
        op = RemoveOrphanFiles(
            catalog=memory_catalog,
            table="nonexistent_namespace.table",
            config=RemoveOrphanFilesConfig(),
        )
        with pytest.raises(CatalogError):
            op.run()
