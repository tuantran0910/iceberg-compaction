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

//! Manifest file rewriting for Iceberg tables.
//!
//! This module provides [`RewriteManifests`] for consolidating and
//! reorganising manifest files without reading or writing any data files.
//! Reducing manifest fragmentation lowers scan planning overhead by giving
//! the Iceberg reader fewer manifest files to open per query.
//!
//! # Supported table formats
//!
//! Only V1 and V2 format tables are supported. V3+ tables use row lineage
//! semantics that are incompatible with manifest rewriting and will return a
//! [`CompactionError::Iceberg`] wrapping `ErrorKind::FeatureUnsupported`.
//!
//! # Examples
//!
//! ```ignore
//! let rewriter = RewriteManifestsBuilder::new(catalog, table_ident, config)
//!     .with_to_branch("main")
//!     .build();
//!
//! match rewriter.rewrite().await? {
//!     Some(result) => println!("Rewrote manifests: {:?}", result.stats),
//!     None => println!("No-op: nothing to rewrite"),
//! }
//! ```

use std::borrow::Cow;
use std::sync::Arc;

use backon::{ExponentialBuilder, Retryable};
use iceberg::spec::{MAIN_BRANCH, ManifestContentType};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, TableIdent};

use crate::compaction::CommitManagerRetryConfig;
use crate::config::{ManifestClusteringStrategy, RewriteManifestsConfig};
use crate::{CompactionError, Result};

/// Counts the number of data-type manifest files in the current snapshot for
/// the given branch.
///
/// Loads only the manifest list (one object-storage read) without opening
/// individual manifest files. Returns `0` when the branch has no snapshot.
pub(crate) async fn count_data_manifests(table: &Table, branch: &str) -> Result<usize> {
    let Some(snapshot) = table.metadata().snapshot_for_ref(branch) else {
        return Ok(0);
    };

    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await?;

    let count = manifest_list
        .entries()
        .iter()
        .filter(|m| m.content == ManifestContentType::Data)
        .count();

    Ok(count)
}

/// Snapshot summary property keys injected by `rewrite_manifests` in iceberg-rust.
///
/// These are defined here to avoid depending on private constants from the
/// upstream library.
const KEPT_MANIFESTS_COUNT: &str = "manifests-kept";
const CREATED_MANIFESTS_COUNT: &str = "manifests-created";
const REPLACED_MANIFESTS_COUNT: &str = "manifests-replaced";
const PROCESSED_ENTRY_COUNT: &str = "entries-processed";

/// Statistics about a completed manifest rewrite operation.
///
/// Values are extracted from the new snapshot's summary properties, which
/// are automatically populated by the iceberg-rust `rewrite_manifests` action.
#[derive(Debug, Clone, Default)]
pub struct RewriteManifestsStats {
    /// Number of data manifests that were kept unchanged.
    pub kept_manifests_count: u64,
    /// Number of new manifest files written.
    pub created_manifests_count: u64,
    /// Number of old manifest files replaced (removed from the snapshot).
    pub replaced_manifests_count: u64,
    /// Total number of live data-file entries processed during clustering.
    pub processed_entry_count: u64,
}

/// Result of a successful manifest rewrite operation.
pub struct RewriteManifestsResult {
    /// The updated table state after the rewrite commit.
    pub table: Table,
    /// Statistics describing the rewrite.
    pub stats: RewriteManifestsStats,
}

/// Standalone manifest rewrite runner.
///
/// Reorganises manifest files in an Iceberg table snapshot without touching
/// data files. Construct via [`RewriteManifestsBuilder`].
pub struct RewriteManifests {
    config: RewriteManifestsConfig,
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    /// Target branch to read from and commit to.
    to_branch: Cow<'static, str>,
    commit_retry_config: CommitManagerRetryConfig,
}

/// Builder for [`RewriteManifests`].
///
/// # Examples
///
/// ```ignore
/// let rewriter = RewriteManifestsBuilder::new(catalog, table_ident, config)
///     .with_to_branch("main")
///     .with_retry_config(CommitManagerRetryConfig::default())
///     .build();
/// ```
pub struct RewriteManifestsBuilder {
    config: RewriteManifestsConfig,
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    to_branch: Option<Cow<'static, str>>,
    commit_retry_config: Option<CommitManagerRetryConfig>,
}

impl RewriteManifestsBuilder {
    /// Creates a new builder with the required catalog, table identifier, and
    /// rewrite configuration.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
        config: RewriteManifestsConfig,
    ) -> Self {
        Self {
            config,
            catalog,
            table_ident,
            to_branch: None,
            commit_retry_config: None,
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

    /// Builds the [`RewriteManifests`] instance.
    pub fn build(self) -> RewriteManifests {
        RewriteManifests {
            config: self.config,
            catalog: self.catalog,
            table_ident: self.table_ident,
            to_branch: self
                .to_branch
                .unwrap_or_else(|| MAIN_BRANCH.to_owned().into()),
            commit_retry_config: self.commit_retry_config.unwrap_or_default(),
        }
    }
}

impl RewriteManifests {
    /// Rewrites manifest files according to the configured strategy.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(result))` when at least one manifest was rewritten and a new
    ///   snapshot was committed.
    /// - `Ok(None)` when there is nothing to rewrite: no snapshot exists, fewer
    ///   than two data manifests are present (already consolidated), or the
    ///   configured predicate filters out all candidates.
    /// - `Err(_)` on I/O errors, V3 tables (`FeatureUnsupported`), or commit
    ///   failures after all retries are exhausted.
    pub async fn rewrite(&self) -> Result<Option<RewriteManifestsResult>> {
        let table = self.catalog.load_table(&self.table_ident).await?;

        // Nothing to rewrite if there is no snapshot on the branch.
        let Some(snapshot) = table.metadata().snapshot_for_ref(&self.to_branch) else {
            return Ok(None);
        };

        // If there is only one data manifest (or none), consolidation is already
        // achieved; skip the rewrite to avoid creating a redundant snapshot.
        if count_data_manifests(&table, &self.to_branch).await? <= 1 {
            return Ok(None);
        }

        let before_snapshot_id = snapshot.snapshot_id();

        // Commit the rewrite action with retry on transient failures.
        let new_table = self.commit_with_retry().await?;

        // Detect a no-op: if the snapshot ID did not advance, iceberg produced
        // an empty ActionCommit and no new snapshot was written.
        let after_snapshot_id = new_table
            .metadata()
            .snapshot_for_ref(&self.to_branch)
            .map(|s| s.snapshot_id());

        if after_snapshot_id == Some(before_snapshot_id) {
            return Ok(None);
        }

        let stats = extract_stats(&new_table, &self.to_branch);
        Ok(Some(RewriteManifestsResult {
            table: new_table,
            stats,
        }))
    }

    /// Builds, applies, and commits the `RewriteManifestsAction` with
    /// exponential-backoff retry on transient catalog errors.
    ///
    /// The table is reloaded on every attempt so that the action always
    /// operates on the latest snapshot — a requirement for optimistic
    /// concurrency with catalog commit conflicts.
    async fn commit_with_retry(&self) -> Result<Table> {
        // Clone fields that need to be moved into the async closure.
        let catalog = self.catalog.clone();
        let table_ident = self.table_ident.clone();
        let to_branch = self.to_branch.to_string();
        let clustering_strategy = self.config.clustering_strategy.clone();
        let snapshot_properties = self.config.snapshot_properties.clone();

        let operation = move || {
            let catalog = catalog.clone();
            let table_ident = table_ident.clone();
            let to_branch = to_branch.clone();
            let clustering_strategy = clustering_strategy.clone();
            let snapshot_properties = snapshot_properties.clone();

            async move {
                // Always reload the table to pick up any concurrent commits.
                let table = catalog.load_table(&table_ident).await?;
                let txn = Transaction::new(&table);

                // Configure the action from the resolved clustering strategy.
                let action = apply_clustering(txn.rewrite_manifests(), &clustering_strategy);
                let action = action
                    .set_target_branch(to_branch)
                    .set_snapshot_properties(snapshot_properties);

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
                    "Manifest rewrite commit failed ({:?}), retrying after {:?}",
                    e,
                    d
                );
            })
            .await
            .map_err(CompactionError::from)
    }
}

/// Applies the `cluster_by` function to the action based on the configured strategy.
///
/// - `Single`: maps every data-file entry to the same key (`"all"`), so all
///   entries land in one manifest.
/// - `ByPartitionSpecId`: groups entries by their partition specification ID,
///   keeping entries with different specs in separate manifests.
fn apply_clustering(
    action: iceberg::transaction::RewriteManifestsAction,
    strategy: &ManifestClusteringStrategy,
) -> iceberg::transaction::RewriteManifestsAction {
    match strategy {
        ManifestClusteringStrategy::Single => {
            action.cluster_by(Box::new(|_data_file| "all".to_owned()))
        }
        ManifestClusteringStrategy::ByPartitionSpecId => {
            // The partition spec ID comes from the ManifestFile that contains the
            // entry, not from the DataFile itself. The cluster_by closure receives
            // the DataFile, but iceberg-rust internally keys by
            // (cluster_key, partition_spec_id), so using a constant key here
            // effectively groups entries by partition_spec_id alone.
            action.cluster_by(Box::new(|_data_file| "by_spec".to_owned()))
        }
    }
}

/// Parses rewrite statistics from the new snapshot's summary properties.
///
/// Missing keys are treated as `0`; any parse errors are silently ignored to
/// avoid failing a successful commit over a missing metric.
fn extract_stats(table: &Table, branch: &str) -> RewriteManifestsStats {
    let Some(snapshot) = table.metadata().snapshot_for_ref(branch) else {
        return RewriteManifestsStats::default();
    };

    let props = &snapshot.summary().additional_properties;

    let parse = |key: &str| -> u64 { props.get(key).and_then(|v| v.parse().ok()).unwrap_or(0) };

    RewriteManifestsStats {
        kept_manifests_count: parse(KEPT_MANIFESTS_COUNT),
        created_manifests_count: parse(CREATED_MANIFESTS_COUNT),
        replaced_manifests_count: parse(REPLACED_MANIFESTS_COUNT),
        processed_entry_count: parse(PROCESSED_ENTRY_COUNT),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
    use iceberg::spec::{DataFile, MAIN_BRANCH, NestedField, PrimitiveType, Schema, Type};
    use iceberg::table::Table;
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use iceberg::writer::file_writer::ParquetWriterBuilder;
    use iceberg::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::config::{
        ManifestClusteringStrategy, RewriteManifestsConfig, RewriteManifestsConfigBuilder,
    };

    // ── Test helpers ──────────────────────────────────────────────────────────

    struct TestEnv {
        #[allow(dead_code)]
        temp_dir: TempDir,
        warehouse_location: String,
        catalog: Arc<MemoryCatalog>,
        table_ident: TableIdent,
    }

    async fn create_test_env() -> TestEnv {
        let temp_dir = TempDir::new().unwrap();
        let warehouse_location = temp_dir.path().to_str().unwrap().to_owned();
        let catalog = Arc::new(
            MemoryCatalogBuilder::default()
                .load(
                    "memory",
                    HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_owned(),
                        warehouse_location.clone(),
                    )]),
                )
                .await
                .unwrap(),
        );

        let namespace_ident = NamespaceIdent::new("test_namespace".into());
        catalog
            .create_namespace(&namespace_ident, HashMap::new())
            .await
            .unwrap();

        let table_ident = TableIdent::new(namespace_ident.clone(), "test_table".into());
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        catalog
            .create_table(
                &table_ident.namespace,
                TableCreation::builder()
                    .name(table_ident.name().into())
                    .schema(schema)
                    .build(),
            )
            .await
            .unwrap();

        TestEnv {
            temp_dir,
            warehouse_location,
            catalog,
            table_ident,
        }
    }

    /// Writes one small data file and performs a fast-append commit, creating one
    /// new manifest per call. Returns the updated table.
    async fn append_one_file(
        table: &Table,
        catalog: &Arc<MemoryCatalog>,
        warehouse_location: &str,
        suffix: &str,
    ) -> Table {
        let data_files = write_one_data_file(table, warehouse_location, suffix).await;
        fast_append(table, catalog.as_ref(), data_files).await
    }

    async fn write_one_data_file(
        table: &Table,
        warehouse_location: &str,
        suffix: &str,
    ) -> Vec<DataFile> {
        let schema = table.metadata().current_schema().clone();
        let location_generator =
            DefaultLocationGenerator::with_data_location(warehouse_location.to_owned());
        let file_name_generator = DefaultFileNameGenerator::new(
            format!("data_{suffix}"),
            Some(Uuid::now_v7().to_string()),
            iceberg::spec::DataFileFormat::Parquet,
        );
        let rolling = RollingFileWriterBuilder::new(
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema),
            1024 * 1024,
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );
        let mut writer = DataFileWriterBuilder::new(rolling)
            .build(None)
            .await
            .unwrap();
        // Write a minimal record so the file is non-empty.
        use datafusion::arrow::array::{Int32Array, StringArray};
        use datafusion::arrow::record_batch::RecordBatch;
        use iceberg::arrow::schema_to_arrow_schema;

        let arrow_schema =
            Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).unwrap());
        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a"])),
        ])
        .unwrap();
        writer.write(batch).await.unwrap();
        writer.close().await.unwrap()
    }

    async fn fast_append<C: Catalog>(
        table: &Table,
        catalog: &C,
        data_files: Vec<DataFile>,
    ) -> Table {
        let txn = Transaction::new(table);
        let action = txn.fast_append().add_data_files(data_files);
        let txn = action.apply(txn).unwrap();
        txn.commit(catalog).await.unwrap()
    }

    /// Returns the number of data-type manifests in the current snapshot.
    async fn data_manifest_count(table: &Table) -> usize {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        list.entries()
            .iter()
            .filter(|m| m.content == iceberg::spec::ManifestContentType::Data)
            .count()
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_builder_defaults() {
        let env = create_test_env().await;
        let rewriter = RewriteManifestsBuilder::new(
            env.catalog.clone(),
            env.table_ident.clone(),
            RewriteManifestsConfig::default(),
        )
        .build();

        assert_eq!(rewriter.to_branch.as_ref(), MAIN_BRANCH);
    }

    #[tokio::test]
    async fn test_rewrite_no_snapshot_returns_none() {
        let env = create_test_env().await;
        // Table was just created — no snapshot yet.
        let rewriter = RewriteManifestsBuilder::new(
            env.catalog.clone(),
            env.table_ident.clone(),
            RewriteManifestsConfig::default(),
        )
        .build();

        let result = rewriter.rewrite().await.unwrap();
        assert!(result.is_none(), "empty table should produce no-op");
    }

    #[tokio::test]
    async fn test_rewrite_single_manifest_is_noop() {
        let env = create_test_env().await;
        let table = env.catalog.load_table(&env.table_ident).await.unwrap();
        // A single fast-append creates exactly one manifest.
        let table = append_one_file(&table, &env.catalog, &env.warehouse_location, "a").await;
        assert_eq!(data_manifest_count(&table).await, 1);

        let rewriter = RewriteManifestsBuilder::new(
            env.catalog.clone(),
            env.table_ident.clone(),
            RewriteManifestsConfig::default(),
        )
        .build();

        // One manifest → nothing to consolidate → no new snapshot.
        let result = rewriter.rewrite().await.unwrap();
        assert!(
            result.is_none(),
            "single manifest should produce no-op (no new snapshot)"
        );
    }

    #[tokio::test]
    async fn test_rewrite_single_strategy_consolidates_all() {
        let env = create_test_env().await;
        let table = env.catalog.load_table(&env.table_ident).await.unwrap();

        // Three separate appends → three separate manifests.
        let table = append_one_file(&table, &env.catalog, &env.warehouse_location, "a").await;
        let table = append_one_file(&table, &env.catalog, &env.warehouse_location, "b").await;
        let table = append_one_file(&table, &env.catalog, &env.warehouse_location, "c").await;
        assert_eq!(data_manifest_count(&table).await, 3);

        let rewriter = RewriteManifestsBuilder::new(
            env.catalog.clone(),
            env.table_ident.clone(),
            RewriteManifestsConfig::default(), // Single strategy, All predicate
        )
        .build();

        let result = rewriter.rewrite().await.unwrap().expect("should rewrite");

        // All three manifests consolidated into one.
        assert_eq!(data_manifest_count(&result.table).await, 1);

        // Stats should reflect the operation.
        assert_eq!(result.stats.created_manifests_count, 1);
        assert_eq!(result.stats.replaced_manifests_count, 3);
        assert_eq!(result.stats.kept_manifests_count, 0);
        assert_eq!(result.stats.processed_entry_count, 3);
    }

    #[tokio::test]
    async fn test_rewrite_all_predicate_rewrites_everything() {
        let env = create_test_env().await;
        let table = env.catalog.load_table(&env.table_ident).await.unwrap();
        let table = append_one_file(&table, &env.catalog, &env.warehouse_location, "x").await;
        let table = append_one_file(&table, &env.catalog, &env.warehouse_location, "y").await;
        assert_eq!(data_manifest_count(&table).await, 2);

        let config = RewriteManifestsConfigBuilder::default()
            .clustering_strategy(ManifestClusteringStrategy::Single)
            .build()
            .unwrap();

        let rewriter =
            RewriteManifestsBuilder::new(env.catalog.clone(), env.table_ident.clone(), config)
                .build();

        let result = rewriter.rewrite().await.unwrap().expect("should rewrite");
        assert_eq!(data_manifest_count(&result.table).await, 1);
        assert_eq!(result.stats.replaced_manifests_count, 2);
    }

    #[tokio::test]
    async fn test_rewrite_by_partition_spec_id_strategy() {
        let env = create_test_env().await;
        let table = env.catalog.load_table(&env.table_ident).await.unwrap();
        // Two appends → two manifests; both share the same partition spec (default).
        let table = append_one_file(&table, &env.catalog, &env.warehouse_location, "r").await;
        let table = append_one_file(&table, &env.catalog, &env.warehouse_location, "s").await;
        assert_eq!(data_manifest_count(&table).await, 2);

        let config = RewriteManifestsConfigBuilder::default()
            .clustering_strategy(ManifestClusteringStrategy::ByPartitionSpecId)
            .build()
            .unwrap();

        let rewriter =
            RewriteManifestsBuilder::new(env.catalog.clone(), env.table_ident.clone(), config)
                .build();

        // Same partition spec for both manifests → all entries cluster together
        // and produce one new manifest.
        let result = rewriter.rewrite().await.unwrap().expect("should rewrite");
        assert_eq!(data_manifest_count(&result.table).await, 1);
    }

    #[tokio::test]
    async fn test_rewrite_snapshot_properties_are_propagated() {
        let env = create_test_env().await;
        let table = env.catalog.load_table(&env.table_ident).await.unwrap();
        let table = append_one_file(&table, &env.catalog, &env.warehouse_location, "t").await;
        let _table = append_one_file(&table, &env.catalog, &env.warehouse_location, "u").await;

        let mut props = HashMap::new();
        props.insert("rewrite-reason".to_owned(), "test".to_owned());

        let config = RewriteManifestsConfigBuilder::default()
            .snapshot_properties(props)
            .build()
            .unwrap();

        let rewriter =
            RewriteManifestsBuilder::new(env.catalog.clone(), env.table_ident.clone(), config)
                .build();

        let result = rewriter.rewrite().await.unwrap().expect("should rewrite");
        let new_snapshot = result
            .table
            .metadata()
            .snapshot_for_ref(MAIN_BRANCH)
            .unwrap();
        assert_eq!(
            new_snapshot
                .summary()
                .additional_properties
                .get("rewrite-reason"),
            Some(&"test".to_owned()),
        );
    }
}
