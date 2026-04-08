"""
Integration tests for Rust → Python error mapping.

These tests verify that errors originating in the Rust layer are correctly
translated to the appropriate Python exception type and that error messages
are preserved through the PyO3 bridge.

Error mapping (from python/src/error.rs):
  CompactionError::Config(_)           → ConfigError
  CompactionError::Iceberg + Conflict   → CommitConflictError
  CompactionError::Iceberg + Unsupported → FeatureUnsupportedError
  CompactionError::Iceberg + other      → CatalogError
  CompactionError::Unexpected(_)        → IcebergCompactionError
  CompactionError::Execution(_)        → IcebergCompactionError
  CompactionError::Io(_)               → IcebergCompactionError
  CompactionError::DataFusion(_)       → IcebergCompactionError

Note: Full end-to-end tests that trigger CommitConflictError and
FeatureUnsupportedError require a real catalog backend (REST/Hive) where
commit conflicts and unsupported feature errors can be induced.  These
Python tests verify the error paths that are reachable with MemoryCatalog.
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
    ConfigError,
    CatalogError,
    CommitConflictError,
    FeatureUnsupportedError,
    IcebergCompactionError,
    MemoryCatalog,
)


class TestConfigErrorMapping:
    """
    Verify ConfigError is raised and messages are preserved for invalid config values.

    Config errors are raised during Config construction (Python → Rust bridge)
    before any operation is run.
    """

    def test_invalid_compaction_strategy_raises_config_error(self) -> None:
        """An invalid strategy string should raise ConfigError."""
        with pytest.raises(ConfigError) as exc_info:
            CompactionConfig(strategy="invalid_strategy")
        assert "invalid_strategy" in str(exc_info.value)

    def test_invalid_grouping_strategy_raises_config_error(self) -> None:
        """An invalid grouping_strategy string should raise ConfigError."""
        with pytest.raises(ConfigError) as exc_info:
            CompactionConfig(grouping_strategy="unknown")
        assert "unknown" in str(exc_info.value)

    def test_zero_max_auto_plans_raises_config_error(self) -> None:
        """max_auto_plans_per_run=0 should raise ConfigError."""
        with pytest.raises(ConfigError) as exc_info:
            CompactionConfig(max_auto_plans_per_run=0)
        assert "max_auto_plans_per_run" in str(exc_info.value)

    def test_invalid_rewrite_manifests_clustering_raises_config_error(self) -> None:
        """An invalid clustering_strategy should raise ConfigError."""
        with pytest.raises(ConfigError) as exc_info:
            RewriteManifestsConfig(clustering_strategy="invalid")
        assert "invalid" in str(exc_info.value)

    def test_config_error_message_preserved(self) -> None:
        """ConfigError message should be accessible from Python."""
        with pytest.raises(ConfigError) as exc_info:
            CompactionConfig(strategy="bad")
        msg = str(exc_info.value)
        assert len(msg) > 0
        assert "bad" in msg


class TestCatalogErrorMapping:
    """
    Verify CatalogError is raised for catalog-level errors.

    Catalog errors occur when the catalog reports a non-conflict error,
    e.g. namespace or table not found.
    """

    @pytest.mark.parametrize(
        "op_class,config_class",
        [
            (Compaction, CompactionConfig),
            (RewriteManifests, RewriteManifestsConfig),
            (ExpireSnapshots, ExpireSnapshotsConfig),
            (RemoveOrphanFiles, RemoveOrphanFilesConfig),
        ],
    )
    def test_missing_namespace_raises_catalog_error(
        self,
        memory_catalog: MemoryCatalog,
        op_class: type,
        config_class: type,
    ) -> None:
        """Operating on a non-existent namespace should raise CatalogError."""
        op = op_class(
            catalog=memory_catalog,
            table="nonexistent_namespace.my_table",
            config=config_class(),
        )
        with pytest.raises(CatalogError) as exc_info:
            op.run()
        assert "nonexistent_namespace" in str(exc_info.value)

    def test_catalog_error_message_preserved(
        self, memory_catalog: MemoryCatalog
    ) -> None:
        """CatalogError message should be accessible from Python."""
        op = Compaction(
            catalog=memory_catalog,
            table="nonexistent_namespace.my_table",
            config=CompactionConfig(),
        )
        with pytest.raises(CatalogError) as exc_info:
            op.run()
        msg = str(exc_info.value)
        assert len(msg) > 0


class TestIcebergCompactionErrorMapping:
    """
    Verify IcebergCompactionError is raised for unexpected errors.

    Unexpected errors include Io errors, execution failures, DataFusion errors,
    and any other CompactionError variant not explicitly mapped above.
    """

    def test_iceberg_compaction_error_inherits_from_exception(self) -> None:
        """IcebergCompactionError should inherit from Exception."""
        assert issubclass(IcebergCompactionError, Exception)

    def test_can_raise_iceberg_compaction_error_directly(self) -> None:
        """IcebergCompactionError can be raised directly."""
        with pytest.raises(IcebergCompactionError) as exc_info:
            raise IcebergCompactionError("test error message")
        assert "test error message" in str(exc_info.value)

    def test_iceberg_compaction_error_message_preserved(self) -> None:
        """IcebergCompactionError message should be accessible from Python."""
        msg = "something went wrong in the compaction engine"
        with pytest.raises(IcebergCompactionError) as exc_info:
            raise IcebergCompactionError(msg)
        assert str(exc_info.value) == msg


class TestErrorHierarchy:
    """
    Verify the exception hierarchy is correct and all error types
    are accessible and usable.
    """

    def test_commit_conflict_error_inherits_from_iceberg_compaction_error(self) -> None:
        """CommitConflictError should inherit from IcebergCompactionError."""
        assert issubclass(CommitConflictError, IcebergCompactionError)

    def test_feature_unsupported_error_inherits_from_iceberg_compaction_error(
        self,
    ) -> None:
        """FeatureUnsupportedError should inherit from IcebergCompactionError."""
        assert issubclass(FeatureUnsupportedError, IcebergCompactionError)

    def test_catalog_error_inherits_from_iceberg_compaction_error(self) -> None:
        """CatalogError should inherit from IcebergCompactionError."""
        assert issubclass(CatalogError, IcebergCompactionError)

    def test_config_error_inherits_from_iceberg_compaction_error(self) -> None:
        """ConfigError should inherit from IcebergCompactionError."""
        assert issubclass(ConfigError, IcebergCompactionError)

    def test_commit_conflict_error_can_be_raised_directly(self) -> None:
        """CommitConflictError can be raised directly with a message."""
        with pytest.raises(CommitConflictError) as exc_info:
            raise CommitConflictError("commit conflict detected")
        assert "commit conflict detected" in str(exc_info.value)

    def test_feature_unsupported_error_can_be_raised_directly(self) -> None:
        """FeatureUnsupportedError can be raised directly with a message."""
        with pytest.raises(FeatureUnsupportedError) as exc_info:
            raise FeatureUnsupportedError("v3 tables not supported")
        assert "v3 tables not supported" in str(exc_info.value)
