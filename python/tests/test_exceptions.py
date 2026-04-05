"""
Tests for the Python exception hierarchy.

All iceberg-compaction exceptions form a chain:
  IcebergCompactionError
    ├── CatalogError
    ├── CommitConflictError
    ├── FeatureUnsupportedError
    └── ConfigError
"""

from __future__ import annotations

from iceberg_compaction import (
    IcebergCompactionError,
    CatalogError,
    CommitConflictError,
    FeatureUnsupportedError,
    ConfigError,
)


class TestExceptionHierarchy:
    """Verify all exceptions inherit from IcebergCompactionError."""

    def test_catalog_error_inherits(self) -> None:
        assert issubclass(CatalogError, IcebergCompactionError)

    def test_commit_conflict_error_inherits(self) -> None:
        assert issubclass(CommitConflictError, IcebergCompactionError)

    def test_feature_unsupported_error_inherits(self) -> None:
        assert issubclass(FeatureUnsupportedError, IcebergCompactionError)

    def test_config_error_inherits(self) -> None:
        assert issubclass(ConfigError, IcebergCompactionError)


class TestExceptionConstruction:
    """Verify exceptions can be constructed with messages."""

    def test_iceberg_compaction_error(self) -> None:
        err = IcebergCompactionError("something went wrong")
        assert str(err) == "something went wrong"

    def test_catalog_error(self) -> None:
        err = CatalogError("namespace not found")
        assert str(err) == "namespace not found"
        assert isinstance(err, Exception)

    def test_commit_conflict_error(self) -> None:
        err = CommitConflictError("commit conflict")
        assert str(err) == "commit conflict"

    def test_feature_unsupported_error(self) -> None:
        err = FeatureUnsupportedError("v1 tables don't support this")
        assert str(err) == "v1 tables don't support this"

    def test_config_error(self) -> None:
        err = ConfigError("invalid value for max_retries: -1")
        assert str(err) == "invalid value for max_retries: -1"
