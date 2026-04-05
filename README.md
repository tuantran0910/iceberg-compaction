# Compaction Runtime for Apache Iceberg™

**Disclaimer:** This project is not affiliated with or endorsed by the Apache Software Foundation. “Apache”, “Apache Iceberg”, and related marks are trademarks of the ASF.

`iceberg-compaction` is a high-performance Rust library for Apache Iceberg™ table maintenance. It provides compaction, manifest rewriting, snapshot expiration, and orphan file removal — all built on [Apache DataFusion](https://datafusion.apache.org/) and [iceberg-rust](https://github.com/apache/iceberg-rust).

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Core Highlights

- **Rust-Native Performance**: Low-latency, high-throughput execution with memory safety guarantees
- **DataFusion Engine**: Leverages Apache DataFusion for query planning and vectorized execution
- **Iceberg Native**: Full compliance with Iceberg table formats via iceberg-rust
- **Multi-Cloud Ready**: Supports AWS S3, with plans for Azure Blob Storage and GCP Cloud Storage

## Table Maintenance

All four table maintenance operations are available as standalone actions. Each follows the same builder pattern and integrates with any `Arc<dyn Catalog>`.

### Rewrite Data Files (Compaction)

Compaction merges small data files and applies pending delete files, reducing storage fragmentation and improving read performance.

Three strategies are available:

| Strategy | When to use |
|---|---|
| `SmallFiles` | Many files smaller than the target file size |
| `FilesWithDeletes` | Files accumulating positional or equality delete files |
| `Full` | Unconditional rewrite of all files in each partition |

`AutoCompaction` selects a strategy automatically based on snapshot statistics.

```rust
use iceberg_compaction_core::{AutoCompaction, AutoCompactionBuilder, AutoCompactionConfig};

let compaction = AutoCompactionBuilder::new(catalog, table_ident)
    .with_config(AutoCompactionConfig::default())
    .build();

let result = compaction.compact().await?;
println!(
    “Compacted {} plans, wrote {} files”,
    result.committed_plans, result.output_files_count
);
```

For fine-grained control, use `CompactionBuilder` directly to plan and commit independently:

```rust
use iceberg_compaction_core::{
    CompactionBuilder, CompactionConfig, CompactionPlanningConfig, SmallFilesConfig,
};

let compaction = CompactionBuilder::new(catalog, table_ident)
    .with_config(CompactionConfig::new(
        CompactionPlanningConfig::SmallFiles(SmallFilesConfig::default()),
        Default::default(),
    ))
    .build();

// Plan, execute, and commit in one call:
let result = compaction.compact().await?;

// Or drive each phase yourself:
let plans = compaction.plan_compaction().await?;
let results = futures::future::join_all(plans.iter().map(|p| compaction.rewrite_plan(p))).await;
let table = compaction.commit_rewrite_results(results).await?;
```

### Rewrite Manifests

Manifest rewriting consolidates fragmented manifest files without touching any data files. Fewer manifests means faster scan planning.

Run this periodically after many small appends or after compaction to keep the manifest list compact.

```rust
use iceberg_compaction_core::{
    RewriteManifests, RewriteManifestsBuilder, RewriteManifestsConfig,
};

let rewriter = RewriteManifestsBuilder::new(catalog, table_ident, RewriteManifestsConfig::default())
    .with_to_branch(“main”)
    .build();

match rewriter.rewrite().await? {
    Some(result) => println!(
        “Rewrote manifests: {} created, {} replaced”,
        result.stats.created_manifests_count, result.stats.replaced_manifests_count
    ),
    None => println!(“No-op: already consolidated”),
}
```

Two clustering strategies control how entries are packed into new manifests:

- `ManifestClusteringStrategy::Single` (default) — all entries into one manifest
- `ManifestClusteringStrategy::ByPartitionSpecId` — one manifest per partition spec, useful after partition evolution

### Expire Snapshots

Snapshot expiration removes old snapshots from table metadata. This is a prerequisite for reclaiming storage: data files only become eligible for orphan deletion once no snapshot references them.

```rust
use iceberg_compaction_core::{ExpireSnapshots, ExpireSnapshotsBuilder, ExpireSnapshotsConfig};
use std::time::Duration;

let config = ExpireSnapshotsConfig {
    retain_last: 5,
    max_snapshot_age: Duration::from_secs(7 * 24 * 3600), // 7 days
    ..Default::default()
};

let expirer = ExpireSnapshotsBuilder::new(catalog, table_ident, config)
    .with_to_branch(“main”)
    .build();

match expirer.expire().await? {
    Some(result) => println!(
        “Expired {} snapshots”,
        result.stats.expired_snapshot_count
    ),
    None => println!(“No-op: nothing to expire”),
}
```

Retention is applied per branch. The `retain_last` most recent snapshots on each branch are always kept, regardless of age. The `main` branch is never fully expired.

### Remove Orphan Files

Orphan file removal deletes files under the table location that are not referenced by any snapshot or table metadata. These accumulate from failed writes, aborted transactions, or after snapshot expiration.

Always run expire snapshots before removing orphan files so that files belonging to recently expired snapshots are not treated as orphans prematurely.

```rust
use iceberg_compaction_core::{RemoveOrphanFiles, RemoveOrphanFilesBuilder, RemoveOrphanFilesConfig};
use std::time::Duration;

let config = RemoveOrphanFilesConfig {
    older_than: Duration::from_secs(7 * 24 * 3600), // 7 days
    dry_run: false,
    ..Default::default()
};

let remover = RemoveOrphanFilesBuilder::new(catalog, table_ident, config).build();
let result = remover.execute().await?;

println!(
    “Found {} orphan files, deleted {}”,
    result.stats.orphan_files_found, result.stats.orphan_files_deleted
);
```

Use `dry_run: true` to identify orphan files before committing to deletion. Files without a last-modified timestamp are always skipped to protect in-progress writes.

### Recommended Maintenance Schedule

Run the four operations in this order to keep tables healthy:

1. **Compaction** — merge data files and apply deletes
2. **Rewrite Manifests** — consolidate the manifest list
3. **Expire Snapshots** — remove old snapshot metadata
4. **Remove Orphan Files** — reclaim storage from unreferenced files

## Examples

### REST Catalog Example

A complete working example using a REST catalog with S3 storage:

```bash
cd examples/rest-catalog
cargo run
```

For details see the [rest-catalog example](./examples/rest-catalog/).

### Python Bindings

All four maintenance operations are available from Python via PyO3. See the [Python Bindings documentation](./docs/python-bindings.md) for installation, configuration, and usage examples.

A complete BigLake example is available at [`python/examples/biglake_metastore_rest_catalog.py`](./python/examples/biglake_metastore_rest_catalog.py).

### Benchmark Tool

See the [bench binary](./integration-tests/src/bin/README.md) for benchmarking compaction performance with synthetic data.

## Development Roadmap

### Performance

- [ ] Partial compaction: incremental compaction strategies
- [ ] Compaction policy: size-based, time-based, and cost-optimized built-in policies
- [ ] Metadata caching: reduce catalog round-trips for repeated operations

### Stability

- [ ] Spill to disk: handle datasets that exceed memory limits
- [ ] Task breakpoint resume: resume operations from failure points
- [ ] E2E test framework: comprehensive testing infrastructure

### Observability

- [ ] Job progress display: real-time progress tracking
- [ ] Richer compaction metrics: per-partition and per-file granularity

### Functionality

- [ ] Sort / ZOrder compaction: data reorganization for improved read locality
- [ ] Clustering: order-by support for co-locating frequently joined data
