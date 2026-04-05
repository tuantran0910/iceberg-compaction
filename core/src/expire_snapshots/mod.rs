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

//! Snapshot expiration for Iceberg tables.
//!
//! This module provides [`ExpireSnapshots`] for removing old snapshots from an
//! Iceberg table's metadata. Expiring snapshots reduces catalog bloat and enables
//! orphan file cleanup by allowing unreachable data files to be identified.
//!
//! # Examples
//!
//! ```ignore
//! let expirer = ExpireSnapshotsBuilder::new(catalog, table_ident, config)
//!     .with_to_branch("main")
//!     .build();
//!
//! match expirer.expire().await? {
//!     Some(result) => println!("Expired {} snapshots", result.stats.expired_snapshot_count),
//!     None => println!("No-op: nothing to expire"),
//! }
//! ```

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Instant;

use backon::{ExponentialBuilder, Retryable};
use iceberg::spec::MAIN_BRANCH;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, TableIdent};
use mixtrics::metrics::BoxedRegistry;
use mixtrics::registry::noop::NoopMetricsRegistry;

use crate::common::metrics::{CompactionMetricsRecorder, Metrics};
use crate::compaction::CommitManagerRetryConfig;
use crate::config::ExpireSnapshotsConfig;
use crate::{CompactionError, Result};

/// Statistics about a completed snapshot expiration.
#[derive(Debug, Clone, Default)]
pub struct ExpireSnapshotsStats {
    /// Number of snapshots removed from the table.
    pub expired_snapshot_count: u64,
}

/// Result of a successful snapshot expiration.
pub struct ExpireSnapshotsResult {
    /// The updated table state after expiration.
    pub table: Table,
    /// Statistics describing the expiration.
    pub stats: ExpireSnapshotsStats,
}

/// Standalone snapshot expiration runner.
///
/// Expires old snapshots from an Iceberg table according to the configured
/// retention policy. Construct via [`ExpireSnapshotsBuilder`].
pub struct ExpireSnapshots {
    config: ExpireSnapshotsConfig,
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    /// Target branch to read from and commit to.
    to_branch: Cow<'static, str>,
    commit_retry_config: CommitManagerRetryConfig,
    metrics: Arc<Metrics>,
    catalog_name: Cow<'static, str>,
    table_ident_name: Cow<'static, str>,
}

/// Builder for [`ExpireSnapshots`].
///
/// # Examples
///
/// ```ignore
/// let expirer = ExpireSnapshotsBuilder::new(catalog, table_ident, config)
///     .with_to_branch("main")
///     .with_retry_config(CommitManagerRetryConfig::default())
///     .build();
/// ```
pub struct ExpireSnapshotsBuilder {
    config: ExpireSnapshotsConfig,
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    to_branch: Option<Cow<'static, str>>,
    commit_retry_config: Option<CommitManagerRetryConfig>,
    metrics: Option<Arc<Metrics>>,
    catalog_name: Option<Cow<'static, str>>,
}

impl ExpireSnapshotsBuilder {
    /// Creates a new builder with the required catalog, table identifier, and
    /// expiration configuration.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
        config: ExpireSnapshotsConfig,
    ) -> Self {
        Self {
            config,
            catalog,
            table_ident,
            to_branch: None,
            commit_retry_config: None,
            metrics: None,
            catalog_name: None,
        }
    }

    /// Sets the target branch. Defaults to [`MAIN_BRANCH`] if not set.
    pub fn with_to_branch(mut self, to_branch: impl Into<Cow<'static, str>>) -> Self {
        self.to_branch = Some(to_branch.into());
        self
    }

    /// Sets commit retry configuration for transient failures.
    /// Defaults to [`CommitManagerRetryConfig::default`] if not set.
    pub fn with_retry_config(mut self, retry_config: CommitManagerRetryConfig) -> Self {
        self.commit_retry_config = Some(retry_config);
        self
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

    /// Builds the [`ExpireSnapshots`] instance.
    pub fn build(self) -> ExpireSnapshots {
        let metrics = if let Some(metrics) = self.metrics {
            metrics
        } else {
            Arc::new(Metrics::new(Box::new(NoopMetricsRegistry)))
        };

        let catalog_name = self
            .catalog_name
            .unwrap_or_else(|| "default".to_owned().into());

        let table_ident_name = Cow::Owned(self.table_ident.name().to_owned());

        ExpireSnapshots {
            config: self.config,
            catalog: self.catalog,
            table_ident: self.table_ident,
            to_branch: self
                .to_branch
                .unwrap_or_else(|| MAIN_BRANCH.to_owned().into()),
            commit_retry_config: self.commit_retry_config.unwrap_or_default(),
            metrics,
            catalog_name,
            table_ident_name,
        }
    }
}

impl ExpireSnapshots {
    /// Expires snapshots according to the configured retention policy.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(result))` when at least one snapshot was expired.
    /// - `Ok(None)` when there is nothing to expire (no snapshots or no changes).
    /// - `Err(_)` on I/O errors or commit failures after all retries are exhausted.
    pub async fn expire(&self) -> Result<Option<ExpireSnapshotsResult>> {
        let start_time = Instant::now();
        let metrics_recorder = CompactionMetricsRecorder::new(
            self.metrics.clone(),
            self.catalog_name.clone(),
            self.table_ident_name.clone(),
        );
        metrics_recorder.record_expire_snapshots_triggered();

        let table = match self.catalog.load_table(&self.table_ident).await {
            Ok(t) => t,
            Err(e) => {
                metrics_recorder.record_expire_snapshots_failed();
                return Err(e.into());
            }
        };

        // Nothing to expire if the table has no snapshots.
        if table.metadata().snapshot_for_ref(&self.to_branch).is_none()
            && table.metadata().snapshots().count() == 0
        {
            metrics_recorder.record_expire_snapshots_noop(start_time.elapsed().as_millis() as f64);
            return Ok(None);
        }

        let before_snapshot_count = table.metadata().snapshots().count();

        // Compute the expiration threshold once; all retry attempts use the same cutoff.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let expire_older_than_ms = now_ms - self.config.max_snapshot_age.as_millis() as i64;

        let new_table = match self.commit_with_retry(expire_older_than_ms).await {
            Ok(t) => t,
            Err(e) => {
                metrics_recorder.record_expire_snapshots_failed();
                return Err(e);
            }
        };

        let after_snapshot_count = new_table.metadata().snapshots().count();
        let expired_snapshot_count =
            before_snapshot_count.saturating_sub(after_snapshot_count) as u64;

        if expired_snapshot_count == 0 {
            metrics_recorder.record_expire_snapshots_noop(start_time.elapsed().as_millis() as f64);
            return Ok(None);
        }

        let stats = ExpireSnapshotsStats {
            expired_snapshot_count,
        };
        metrics_recorder
            .record_expire_snapshots_success(&stats, start_time.elapsed().as_millis() as f64);

        Ok(Some(ExpireSnapshotsResult {
            table: new_table,
            stats,
        }))
    }

    /// Builds and commits the `RemoveSnapshotAction` with exponential-backoff retry
    /// on transient catalog errors.
    ///
    /// The table is reloaded on every attempt so the action always operates on the
    /// latest snapshot — a requirement for optimistic concurrency with catalog
    /// commit conflicts.
    async fn commit_with_retry(&self, expire_older_than_ms: i64) -> Result<Table> {
        let catalog = self.catalog.clone();
        let table_ident = self.table_ident.clone();
        let config = self.config.clone();

        let operation = move || {
            let catalog = catalog.clone();
            let table_ident = table_ident.clone();
            let config = config.clone();

            async move {
                let table = catalog.load_table(&table_ident).await?;
                let txn = Transaction::new(&table);

                let mut action = txn
                    .expire_snapshot()
                    .retain_last(config.retain_last)
                    .expire_older_than(expire_older_than_ms)
                    .clear_expired_meta_data(config.clear_expired_metadata);

                for &snapshot_id in &config.snapshot_ids_to_expire {
                    action = action.expire_snapshot_id(snapshot_id);
                }

                let txn = action.apply(txn)?;
                txn.commit(catalog.as_ref()).await
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
                    "Expire snapshots commit failed ({:?}), retrying after {:?}",
                    e,
                    d
                );
            })
            .await
            .map_err(CompactionError::from)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::CatalogBuilder;
    use iceberg::memory::MemoryCatalogBuilder;
    use iceberg::spec::MAIN_BRANCH;

    use super::*;
    use crate::config::ExpireSnapshotsConfig;

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
    async fn test_builder_defaults() {
        let (catalog, ident) = make_catalog_and_ident().await;
        let config = ExpireSnapshotsConfig::default();
        let expirer = ExpireSnapshotsBuilder::new(catalog, ident, config).build();
        assert_eq!(expirer.to_branch.as_ref(), MAIN_BRANCH);
    }

    #[tokio::test]
    async fn test_builder_custom_branch() {
        let (catalog, ident) = make_catalog_and_ident().await;
        let config = ExpireSnapshotsConfig::default();
        let expirer = ExpireSnapshotsBuilder::new(catalog, ident, config)
            .with_to_branch("staging")
            .build();
        assert_eq!(expirer.to_branch.as_ref(), "staging");
    }
}
