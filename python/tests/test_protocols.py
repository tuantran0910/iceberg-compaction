"""
Tests for structural protocol classes.

MaintenanceOperation and MaintenanceConfig are @runtime_checkable Protocols
that allow structural typing — any class implementing .run() or being a config
class is accepted.
"""

from __future__ import annotations

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
    MaintenanceOperation,
    MaintenanceConfig,
    MemoryCatalog
)


class TestMaintenanceOperationProtocol:
    """All operation classes satisfy the MaintenanceOperation protocol."""

    def test_compaction_is_operation(
        self, memory_catalog: "MemoryCatalog"
    ) -> None:
        op: MaintenanceOperation = Compaction(
            catalog=memory_catalog,
            table="ns.tbl",
            config=CompactionConfig(),
        )
        assert isinstance(op, MaintenanceOperation)
        assert callable(op.run)

    def test_rewrite_manifests_is_operation(
        self, memory_catalog: "MemoryCatalog"
    ) -> None:
        op: MaintenanceOperation = RewriteManifests(
            catalog=memory_catalog,
            table="ns.tbl",
            config=RewriteManifestsConfig(),
        )
        assert isinstance(op, MaintenanceOperation)
        assert callable(op.run)

    def test_expire_snapshots_is_operation(
        self, memory_catalog: "MemoryCatalog"
    ) -> None:
        op: MaintenanceOperation = ExpireSnapshots(
            catalog=memory_catalog,
            table="ns.tbl",
            config=ExpireSnapshotsConfig(),
        )
        assert isinstance(op, MaintenanceOperation)
        assert callable(op.run)

    def test_remove_orphan_files_is_operation(
        self, memory_catalog: "MemoryCatalog"
    ) -> None:
        op: MaintenanceOperation = RemoveOrphanFiles(
            catalog=memory_catalog,
            table="ns.tbl",
            config=RemoveOrphanFilesConfig(),
        )
        assert isinstance(op, MaintenanceOperation)
        assert callable(op.run)


class TestMaintenanceConfigProtocol:
    """All *Config classes satisfy the MaintenanceConfig protocol."""

    def test_compaction_config_is_config(self) -> None:
        cfg: MaintenanceConfig = CompactionConfig()
        assert isinstance(cfg, MaintenanceConfig)

    def test_rewrite_manifests_config_is_config(self) -> None:
        cfg: MaintenanceConfig = RewriteManifestsConfig()
        assert isinstance(cfg, MaintenanceConfig)

    def test_expire_snapshots_config_is_config(self) -> None:
        cfg: MaintenanceConfig = ExpireSnapshotsConfig()
        assert isinstance(cfg, MaintenanceConfig)

    def test_remove_orphan_files_config_is_config(self) -> None:
        cfg: MaintenanceConfig = RemoveOrphanFilesConfig()
        assert isinstance(cfg, MaintenanceConfig)

    def test_retry_config_is_config(self) -> None:
        cfg: MaintenanceConfig = RetryConfig()
        assert isinstance(cfg, MaintenanceConfig)
