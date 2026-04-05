# Python Bindings

The `iceberg-compaction` Python package provides access to all four maintenance operations from Python, built on PyO3.

## Installation

```bash
pip install iceberg-compaction
```

Or install from source:

```bash
cd python
maturin develop --uv
```

## Quick Start

```python
from iceberg_compaction import (
    Compaction, CompactionConfig,
    ExpireSnapshots, ExpireSnapshotsConfig,
    RemoveOrphanFiles, RemoveOrphanFilesConfig,
    RewriteManifests, RewriteManifestsConfig,
    RestCatalog,
)

# Create catalog
cat = RestCatalog(
    name="my_catalog",
    uri="https://iceberg.example.com/iceberg/v1/restcatalog",
    warehouse="s3://my-warehouse/",
    properties={
        "s3.access-key-id": "...",
        "s3.secret-access-key": "...",
    },
)

# Compaction
result = Compaction(
    cat,
    "my_namespace.my_table",
    CompactionConfig(strategy="small_files", target_file_size_bytes=128 * 1024 * 1024),
).run()

# Expire snapshots
result = ExpireSnapshots(
    cat,
    "my_namespace.my_table",
    ExpireSnapshotsConfig(retain_last=2, max_snapshot_age_secs=86400),
).run()

# Remove orphan files
result = RemoveOrphanFiles(
    cat,
    "my_namespace.my_table",
    RemoveOrphanFilesConfig(older_than_secs=86400, dry_run=False),
).run()

# Rewrite manifests
result = RewriteManifests(
    cat,
    "my_namespace.my_table",
    RewriteManifestsConfig(),
).run()
```

## Supported Catalogs

The Python bindings support these catalog types via `iceberg_compaction`:

| Catalog | Class | Notes |
|---------|-------|-------|
| REST Catalog | `RestCatalog` | Supports any REST-compliant catalog (AWS Glue, BigLake, etc.) |
| AWS Glue | `GlueCatalog` | Uses AWS SDK, credentials via standard AWS config |
| Apache Hive Metastore | `HmsCatalog` | Thrift-based HMS connection |
| S3 Tables | `S3TablesCatalog` | AWS S3 Tables REST API |
| Memory Catalog | `MemoryCatalog` | In-memory, for testing only |

### REST Catalog Configuration

```python
# AWS BigLake / Google Cloud
cat = RestCatalog(
    name="my_biglake",
    uri="https://biglake.googleapis.com/iceberg/v1/restcatalog",
    warehouse="bq://project/dataset",
    properties={
        "token": "<oauth2_token>",
        "gcs.project-id": "my-project",
    },
)

# Generic REST with AWS S3
cat = RestCatalog(
    name="my_rest",
    uri="https://catalog.example.com/iceberg/v1/restcatalog",
    warehouse="s3://my-bucket/warehouse/",
    properties={
        "token": "<bearer_token>",
        "s3.access-key-id": "AKIA...",
        "s3.secret-access-key": "...",
        "s3.region": "us-east-1",
    },
)
```

## Configuration Reference

### CompactionConfig

```python
CompactionConfig(
    strategy="auto" | "small_files" | "files_with_deletes" | "full",
    target_file_size_bytes=128 * 1024 * 1024,  # 128 MB default
    split_size_bytes=134217728,                 # DataFusion split size
    min_file_size_bytes=4 * 1024 * 1024,       # 4 MB minimum
    max_concurrent_rewrite=4,
    max_auto_plans_per_run=10,
)
```

### ExpireSnapshotsConfig

```python
ExpireSnapshotsConfig(
    retain_last=5,                    # Keep last N snapshots per branch
    max_snapshot_age_secs=604800,    # 7 days default
    expire_older_than=None,          # Override: expire snapshots older than timestamp (ms)
    snapshot_ids_to_expire=[],       # Force-expire specific snapshot IDs
    clear_expired_metadata=True,
)
```

### RemoveOrphanFilesConfig

```python
RemoveOrphanFilesConfig(
    older_than_secs=604800,          # 7 days default
    dry_run=True,                    # True = list only, False = delete
    load_concurrency=10,
    delete_concurrency=10,
)
```

### RewriteManifestsConfig

```python
RewriteManifestsConfig(
    strategy="single" | "by_partition_spec_id",
    rewrite_interval_secs=None,      # Auto-rewrite if manifest older than N seconds
    target_manifest_size_bytes=8 * 1024 * 1024,  # 8 MB target
)
```

## Error Handling

All operations raise Python exceptions mapped from Rust errors:

| Exception | Cause |
|-----------|-------|
| `ConfigError` | Invalid configuration values |
| `CatalogError` | Table/namespace not found, access denied |
| `CommitConflictError` | Optimistic lock conflict, retry needed |
| `FeatureUnsupportedError` | Unsupported Iceberg feature |
| `IcebergCompactionError` | Base exception for other errors |

```python
from iceberg_compaction import (
    ConfigError, CatalogError, CommitConflictError,
    IcebergCompactionError,
)

try:
    result = Compaction(cat, "ns.table", config).run()
except CommitConflictError as e:
    print(f"Commit conflict: {e}, retry recommended")
except CatalogError as e:
    print(f"Table not found: {e}")
except IcebergCompactionError as e:
    print(f"Operation failed: {e}")
```

## Limitations

When used from Python, this crate has the following limitations:

| Limitation | Description |
|------------|-------------|
| Blocking catalog constructors | Catalog constructors block the calling Python thread during network I/O because the GIL is held for the entire duration |
| Global Tokio runtime | A single global Tokio runtime is created on first use; if multi-threaded creation fails, falls back to single-threaded |
| Synchronous operations | All operations are synchronous from Python; there is no async/await interface, so operations block the calling thread |
| Limited catalog types | Only five catalog types are available: `MemoryCatalog`, `RestCatalog`, `GlueCatalog`, `HmsCatalog`, and `S3TablesCatalog` |

## Metrics

Metrics are recorded via the `mixtrics` library. To enable metrics, pass a metrics registry:

```python
from mixtrics import PrometheusRegistry
from iceberg_compaction import Compaction

registry = PrometheusRegistry()

compaction = Compaction(
    cat, "ns.table", config,
    metrics=registry,
    catalog_name="my_catalog",
)
```

Without a metrics registry, all metric calls are no-ops (no performance impact).

## Examples

See [`python/examples/biglake_metastore_rest_catalog.py`](./examples/biglake_metastore_rest_catalog.py) for a complete end-to-end example using Google BigLake with PyIceberg for table setup.
