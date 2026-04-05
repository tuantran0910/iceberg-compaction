"""
iceberg-compaction — Python bindings for Iceberg table maintenance operations.

All four maintenance operations are exposed with a uniform ``.run()`` interface::

    from iceberg_compaction import (
        RestCatalog,
        Compaction, CompactionConfig,
        RewriteManifests, RewriteManifestsConfig,
        ExpireSnapshots, ExpireSnapshotsConfig,
        RemoveOrphanFiles, RemoveOrphanFilesConfig,
    )

    catalog = RestCatalog("my_catalog", uri="https://...", warehouse="s3://...")

    # Compact data files
    result = Compaction(catalog, "ns.table", CompactionConfig()).run()

    # Rewrite manifests
    result = RewriteManifests(catalog, "ns.table", RewriteManifestsConfig()).run()

    # Expire old snapshots
    result = ExpireSnapshots(catalog, "ns.table", ExpireSnapshotsConfig(retain_last=5)).run()

    # Remove orphan files
    result = RemoveOrphanFiles(catalog, "ns.table", RemoveOrphanFilesConfig()).run()
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from iceberg_compaction._internal import (  # noqa: F401
    # Catalogs
    GlueCatalog,
    HmsCatalog,
    MemoryCatalog,
    RestCatalog,
    S3TablesCatalog,
    # Configs
    CompactionConfig,
    ExpireSnapshotsConfig,
    RemoveOrphanFilesConfig,
    RewriteManifestsConfig,
    RetryConfig,
    # Operations
    Compaction,
    ExpireSnapshots,
    RemoveOrphanFiles,
    RewriteManifests,
    # Results
    CompactionResult,
    ExpireSnapshotsResult,
    RemoveOrphanFilesResult,
    RewriteManifestsResult,
    # Exceptions
    CatalogError,
    CommitConflictError,
    ConfigError,
    FeatureUnsupportedError,
    IcebergCompactionError,
)


@runtime_checkable
class MaintenanceOperation(Protocol):
    """Structural protocol satisfied by all operation classes.

    Allows treating operations uniformly::

        ops: list[MaintenanceOperation] = [
            Compaction(catalog, table, CompactionConfig()),
            ExpireSnapshots(catalog, table, ExpireSnapshotsConfig()),
            RemoveOrphanFiles(catalog, table, RemoveOrphanFilesConfig()),
        ]
        for op in ops:
            op.run()
    """

    def run(self) -> object: ...


@runtime_checkable
class MaintenanceConfig(Protocol):
    """Marker protocol satisfied by all ``*Config`` classes."""

    pass


__all__ = [
    # Catalogs
    "GlueCatalog",
    "HmsCatalog",
    "MemoryCatalog",
    "RestCatalog",
    "S3TablesCatalog",
    # Configs
    "CompactionConfig",
    "ExpireSnapshotsConfig",
    "RemoveOrphanFilesConfig",
    "RewriteManifestsConfig",
    "RetryConfig",
    # Operations
    "Compaction",
    "ExpireSnapshots",
    "RemoveOrphanFiles",
    "RewriteManifests",
    # Results
    "CompactionResult",
    "ExpireSnapshotsResult",
    "RemoveOrphanFilesResult",
    "RewriteManifestsResult",
    # Exceptions
    "CatalogError",
    "CommitConflictError",
    "ConfigError",
    "FeatureUnsupportedError",
    "IcebergCompactionError",
    # Protocols
    "MaintenanceOperation",
    "MaintenanceConfig",
]
