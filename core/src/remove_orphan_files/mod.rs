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

//! Orphan file removal for Iceberg tables.
//!
//! This module provides [`RemoveOrphanFiles`] for deleting files under the
//! table location that are not referenced by any snapshot or table metadata.
//! Running this after snapshot expiration reclaims storage from data files
//! that are no longer reachable.
//!
//! Files without a last-modified timestamp are always skipped to protect
//! in-progress writes.
//!
//! # Examples
//!
//! ```ignore
//! let remover = RemoveOrphanFilesBuilder::new(catalog, table_ident, config)
//!     .build();
//!
//! let result = remover.execute().await?;
//! println!("Deleted {} orphan files", result.stats.orphan_files_deleted);
//! ```

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Instant;

use backon::{ExponentialBuilder, Retryable};
use iceberg::actions::RemoveOrphanFilesAction;
use iceberg::{Catalog, TableIdent};
use mixtrics::metrics::BoxedRegistry;
use mixtrics::registry::noop::NoopMetricsRegistry;

use crate::Result;
use crate::common::metrics::{CompactionMetricsRecorder, Metrics};
use crate::compaction::CommitManagerRetryConfig;
use crate::config::RemoveOrphanFilesConfig;

/// Statistics about a completed orphan file removal.
#[derive(Debug, Clone, Default)]
pub struct RemoveOrphanFilesStats {
    /// Number of orphan files identified under the table location.
    pub orphan_files_found: u64,
    /// Number of orphan files deleted. Zero when `dry_run` is `true`.
    pub orphan_files_deleted: u64,
}

/// Result of an orphan file removal operation.
pub struct RemoveOrphanFilesResult {
    /// Paths of identified orphan files (deleted or found in dry-run mode).
    pub orphan_file_paths: Vec<String>,
    /// Statistics describing the operation.
    pub stats: RemoveOrphanFilesStats,
}

/// Standalone orphan file removal runner.
///
/// Identifies and optionally deletes files under the table location that are
/// not referenced by any live snapshot or table metadata. Construct via
/// [`RemoveOrphanFilesBuilder`].
pub struct RemoveOrphanFiles {
    config: RemoveOrphanFilesConfig,
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    metrics: Arc<Metrics>,
    catalog_name: Cow<'static, str>,
    table_ident_name: Cow<'static, str>,
    commit_retry_config: CommitManagerRetryConfig,
}

/// Builder for [`RemoveOrphanFiles`].
///
/// # Examples
///
/// ```ignore
/// let remover = RemoveOrphanFilesBuilder::new(catalog, table_ident, config).build();
/// ```
pub struct RemoveOrphanFilesBuilder {
    config: RemoveOrphanFilesConfig,
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    metrics: Option<Arc<Metrics>>,
    catalog_name: Option<Cow<'static, str>>,
    commit_retry_config: Option<CommitManagerRetryConfig>,
}

impl RemoveOrphanFilesBuilder {
    /// Creates a new builder with the required catalog, table identifier, and
    /// removal configuration.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
        config: RemoveOrphanFilesConfig,
    ) -> Self {
        Self {
            config,
            catalog,
            table_ident,
            metrics: None,
            catalog_name: None,
            commit_retry_config: None,
        }
    }

    /// Sets the metrics registry. Defaults to `NoopMetricsRegistry`.
    pub fn with_metrics(mut self, registry: BoxedRegistry) -> Self {
        self.metrics = Some(Arc::new(Metrics::new(registry)));
        self
    }

    /// Sets the catalog name for metrics labels.
    pub fn with_catalog_name(mut self, catalog_name: impl Into<Cow<'static, str>>) -> Self {
        self.catalog_name = Some(catalog_name.into());
        self
    }

    /// Sets the commit retry configuration for handling transient catalog errors.
    pub fn with_retry_config(mut self, retry_config: CommitManagerRetryConfig) -> Self {
        self.commit_retry_config = Some(retry_config);
        self
    }

    /// Builds the [`RemoveOrphanFiles`] instance.
    pub fn build(self) -> RemoveOrphanFiles {
        let metrics = if let Some(metrics) = self.metrics {
            metrics
        } else {
            Arc::new(Metrics::new(Box::new(NoopMetricsRegistry)))
        };

        let catalog_name = self
            .catalog_name
            .unwrap_or_else(|| "default".to_owned().into());

        let table_ident_name = Cow::Owned(self.table_ident.name().to_owned());

        RemoveOrphanFiles {
            config: self.config,
            catalog: self.catalog,
            table_ident: self.table_ident,
            metrics,
            catalog_name,
            table_ident_name,
            commit_retry_config: self.commit_retry_config.unwrap_or_default(),
        }
    }
}

impl RemoveOrphanFiles {
    /// Identifies and optionally deletes orphan files under the table location.
    ///
    /// Returns all orphan file paths found. When `dry_run` is `true`, no files
    /// are deleted and `stats.orphan_files_deleted` is `0`.
    pub async fn execute(&self) -> Result<RemoveOrphanFilesResult> {
        let start_time = Instant::now();
        let metrics_recorder = CompactionMetricsRecorder::new(
            self.metrics.clone(),
            self.catalog_name.clone(),
            self.table_ident_name.clone(),
        );
        metrics_recorder.record_remove_orphan_files_triggered();

        let orphan_file_paths = match self.commit_with_retry().await {
            Ok(paths) => paths,
            Err(e) => {
                metrics_recorder.record_remove_orphan_files_failed();
                return Err(e);
            }
        };

        let found = orphan_file_paths.len() as u64;
        let deleted = if self.config.dry_run { 0 } else { found };

        let stats = RemoveOrphanFilesStats {
            orphan_files_found: found,
            orphan_files_deleted: deleted,
        };

        metrics_recorder
            .record_remove_orphan_files_success(&stats, start_time.elapsed().as_millis() as f64);

        Ok(RemoveOrphanFilesResult {
            orphan_file_paths,
            stats,
        })
    }

    async fn commit_with_retry(&self) -> Result<Vec<String>> {
        let catalog = self.catalog.clone();
        let table_ident = self.table_ident.clone();
        let config = self.config.clone();

        let operation = move || {
            let catalog = catalog.clone();
            let table_ident = table_ident.clone();
            let config = config.clone();

            async move {
                let table = catalog.load_table(&table_ident).await?;
                RemoveOrphanFilesAction::new(table)
                    .older_than(config.older_than)
                    .dry_run(config.dry_run)
                    .load_concurrency(config.load_concurrency)
                    .delete_concurrency(config.delete_concurrency)
                    .execute()
                    .await
            }
        };

        let retry_strategy = ExponentialBuilder::default()
            .with_min_delay(self.commit_retry_config.retry_initial_delay)
            .with_max_delay(self.commit_retry_config.retry_max_delay)
            .with_max_times(self.commit_retry_config.max_retries as usize);

        operation
            .retry(retry_strategy)
            .when(|e| {
                matches!(e.kind(), iceberg::ErrorKind::DataInvalid)
                    || matches!(e.kind(), iceberg::ErrorKind::Unexpected)
                    || matches!(e.kind(), iceberg::ErrorKind::CatalogCommitConflicts)
            })
            .notify(|e, d| {
                tracing::info!(
                    "Remove orphan files commit failed ({:?}), retrying after {:?}",
                    e,
                    d
                );
            })
            .await
            .map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use iceberg::CatalogBuilder;
    use iceberg::memory::MemoryCatalogBuilder;

    use super::*;
    use crate::config::{
        DEFAULT_REMOVE_ORPHAN_FILES_DELETE_CONCURRENCY,
        DEFAULT_REMOVE_ORPHAN_FILES_LOAD_CONCURRENCY, DEFAULT_REMOVE_ORPHAN_FILES_OLDER_THAN_MS,
        RemoveOrphanFilesConfig,
    };

    async fn make_catalog_and_ident() -> (Arc<dyn Catalog>, TableIdent) {
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([("warehouse".to_owned(), "/tmp/iceberg-test".to_owned())]),
            )
            .await
            .unwrap();
        let ident = TableIdent::from_strs(["ns", "test"]).unwrap();
        (Arc::new(catalog), ident)
    }

    #[tokio::test]
    async fn test_builder_creates_instance() {
        let (catalog, ident) = make_catalog_and_ident().await;
        let config = RemoveOrphanFilesConfig::default();
        // Verify builder produces a RemoveOrphanFiles instance without panicking.
        let _ = RemoveOrphanFilesBuilder::new(catalog, ident, config).build();
    }

    #[test]
    fn test_default_config() {
        let config = RemoveOrphanFilesConfig::default();
        assert_eq!(
            config.older_than,
            Duration::from_millis(DEFAULT_REMOVE_ORPHAN_FILES_OLDER_THAN_MS)
        );
        assert!(!config.dry_run);
        assert_eq!(
            config.load_concurrency,
            DEFAULT_REMOVE_ORPHAN_FILES_LOAD_CONCURRENCY
        );
        assert_eq!(
            config.delete_concurrency,
            DEFAULT_REMOVE_ORPHAN_FILES_DELETE_CONCURRENCY
        );
    }
}
