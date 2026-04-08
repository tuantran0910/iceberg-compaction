/*
 * Copyright 2025 iceberg-compaction
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Table maintenance for Apache Iceberg™ tables.
//!
//! This crate provides four standalone maintenance operations:
//!
//! | Operation | Entry point | What it does |
//! |---|---|---|
//! | Rewrite data files | [`AutoCompaction`] / [`compaction::Compaction`] | Merges small files, applies deletes |
//! | Rewrite manifests | [`RewriteManifests`] | Consolidates manifest files (metadata only) |
//! | Expire snapshots | [`ExpireSnapshots`] | Removes old snapshot metadata |
//! | Remove orphan files | [`RemoveOrphanFiles`] | Deletes unreferenced files from storage |
//!
//! All operations share the same builder pattern:
//!
//! ```ignore
//! let action = ActionBuilder::new(catalog, table_ident, config).build();
//! let result = action.run().await?;
//! ```
//!
//! # Recommended execution order
//!
//! Run the operations in this sequence to keep tables healthy without
//! accidentally deleting files that are still reachable:
//!
//! 1. **Rewrite data files** — compact small files and apply pending deletes
//! 2. **Rewrite manifests** — consolidate the manifest list
//! 3. **Expire snapshots** — remove old snapshot entries from metadata
//! 4. **Remove orphan files** — reclaim storage from files no longer referenced
//!
//! # Error handling
//!
//! All fallible operations return [`Result<T>`], which is an alias for
//! `std::result::Result<T, `[`CompactionError`]`>`. Transactional operations
//! (data file rewrite, manifest rewrite, snapshot expiration) retry
//! automatically on catalog commit conflicts using exponential backoff.
//!
//! # Python Binding Limitations
//!
//! When used from Python via `PyO3`, this crate has the following limitations:
//!
//! | Limitation | Description |
//! |---|---|
//! | Blocking catalog constructors | Catalog constructors block the calling Python thread during network I/O because the GIL is held for the entire duration |
//! | Global Tokio runtime | A single global Tokio runtime is created on first use; if multi-threaded creation fails, falls back to single-threaded |
//! | Synchronous operations | All operations are synchronous from Python; there is no async/await interface, so operations block the calling thread |
//! | Limited catalog types | Only five catalog types are available: `MemoryCatalog`, `RestCatalog`, `GlueCatalog`, `HmsCatalog`, and `S3TablesCatalog` |

pub mod common;
pub mod compaction;
pub mod config;
pub mod error;
pub mod executor;
pub mod expire_snapshots;
pub mod file_selection;
pub mod manifest_rewrite;
pub mod remove_orphan_files;

pub use compaction::{AutoCompaction, AutoCompactionBuilder};
pub use config::{
    AutoCompactionConfig, AutoThresholds, CompactionConfig, ExpireSnapshotsConfig,
    ExpireSnapshotsConfigBuilder, ManifestClusteringStrategy, RemoveOrphanFilesConfig,
    RemoveOrphanFilesConfigBuilder, RewriteManifestsConfig, RewriteManifestsConfigBuilder,
};
pub use error::{CompactionError, Result};
pub use executor::CompactionExecutor;
pub use expire_snapshots::{
    ExpireSnapshots, ExpireSnapshotsBuilder, ExpireSnapshotsResult, ExpireSnapshotsStats,
};
pub use file_selection::SnapshotStats;
// Re-export iceberg related crates
pub use iceberg;
// pub use iceberg_catalog_memory;
pub use iceberg_catalog_rest;
pub use manifest_rewrite::{
    RewriteManifests, RewriteManifestsBuilder, RewriteManifestsResult, RewriteManifestsStats,
};
pub use remove_orphan_files::{
    RemoveOrphanFiles, RemoveOrphanFilesBuilder, RemoveOrphanFilesResult, RemoveOrphanFilesStats,
};
