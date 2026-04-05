use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use iceberg_compaction_core::compaction::{
    AutoCompaction, AutoCompactionBuilder, Compaction, CompactionBuilder,
};
use iceberg_compaction_core::config::{
    AutoCompactionConfig, AutoCompactionConfigBuilder, AutoThresholds, BinPackConfig,
    CompactionConfig, CompactionExecutionConfigBuilder, CompactionPlanningConfig,
    FilesWithDeletesConfigBuilder, FullCompactionConfigBuilder, GroupFilters, GroupFiltersBuilder,
    GroupingStrategy, ManifestClusteringStrategy, RewriteManifestsConfig,
    RewriteManifestsConfigBuilder, SmallFilesConfigBuilder,
};
use pyo3::prelude::*;

use crate::catalog::{CatalogRef, parse_table_ident};
use crate::error::{ConfigError, map_err};
use crate::manifest_rewrite::PyRewriteManifestsOpResult;
use crate::runtime;

// ── RetryConfig ──────────────────────────────────────────────────────────────

/// Retry configuration for transactional catalog commits.
#[pyclass(name = "RetryConfig")]
#[derive(Clone)]
pub struct PyRetryConfig {
    #[pyo3(get)]
    pub max_retries: u32,
    #[pyo3(get)]
    pub initial_delay_secs: u64,
    #[pyo3(get)]
    pub max_delay_secs: u64,
}

#[pymethods]
impl PyRetryConfig {
    #[new]
    #[pyo3(signature = (max_retries = 3, initial_delay_secs = 1, max_delay_secs = 10))]
    fn new(max_retries: u32, initial_delay_secs: u64, max_delay_secs: u64) -> Self {
        Self {
            max_retries,
            initial_delay_secs,
            max_delay_secs,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "RetryConfig(max_retries={}, initial_delay_secs={}, max_delay_secs={})",
            self.max_retries, self.initial_delay_secs, self.max_delay_secs
        )
    }
}

impl PyRetryConfig {
    pub(crate) fn to_rust(&self) -> iceberg_compaction_core::compaction::CommitManagerRetryConfig {
        iceberg_compaction_core::compaction::CommitManagerRetryConfig {
            max_retries: self.max_retries,
            retry_initial_delay: Duration::from_secs(self.initial_delay_secs),
            retry_max_delay: Duration::from_secs(self.max_delay_secs),
        }
    }
}

// ── CompactionConfig ─────────────────────────────────────────────────────────

/// Configuration for data file compaction.
///
/// The ``strategy`` parameter selects the planning algorithm:
///
/// * ``"auto"`` (default) — automatically selects the best strategy based on
///   table statistics (``FilesWithDeletes`` → ``SmallFiles`` → no-op).
/// * ``"small_files"`` — compact files smaller than ``small_file_threshold_bytes``.
/// * ``"full"`` — rewrite all data files unconditionally.
/// * ``"files_with_deletes"`` — compact files that have pending delete files.
#[pyclass(name = "CompactionConfig")]
#[derive(Clone)]
pub struct PyCompactionConfig {
    #[pyo3(get)]
    pub strategy: String,
    #[pyo3(get)]
    pub target_file_size_bytes: u64,
    #[pyo3(get)]
    pub small_file_threshold_bytes: u64,
    #[pyo3(get)]
    pub min_size_per_partition: u64,
    #[pyo3(get)]
    pub max_file_count_per_partition: usize,
    #[pyo3(get)]
    pub max_input_parallelism: Option<usize>,
    #[pyo3(get)]
    pub max_output_parallelism: Option<usize>,
    #[pyo3(get)]
    pub grouping_strategy: String,
    #[pyo3(get)]
    pub bin_pack_target_size_bytes: Option<u64>,
    #[pyo3(get)]
    pub min_group_size_bytes: Option<u64>,
    #[pyo3(get)]
    pub min_group_file_count: Option<usize>,
    #[pyo3(get)]
    pub min_delete_file_count_threshold: usize,
    #[pyo3(get)]
    pub data_file_prefix: String,
    #[pyo3(get)]
    pub max_record_batch_rows: usize,
    #[pyo3(get)]
    pub max_concurrent_closes: usize,
    #[pyo3(get)]
    pub enable_validate_compaction: bool,
    #[pyo3(get)]
    pub enable_prefetch: bool,
    #[pyo3(get)]
    pub auto_min_small_files_count: usize,
    #[pyo3(get)]
    pub auto_min_delete_heavy_files_count: usize,
    #[pyo3(get)]
    pub max_auto_plans_per_run: Option<usize>,
}

#[pymethods]
impl PyCompactionConfig {
    #[new]
    #[pyo3(signature = (
        strategy = "auto",
        target_file_size_bytes = 1_073_741_824,
        small_file_threshold_bytes = 33_554_432,
        min_size_per_partition = 536_870_912,
        max_file_count_per_partition = 32,
        max_input_parallelism = None,
        max_output_parallelism = None,
        grouping_strategy = "single",
        bin_pack_target_size_bytes = None,
        min_group_size_bytes = None,
        min_group_file_count = None,
        min_delete_file_count_threshold = 128,
        data_file_prefix = "iceberg-compact",
        max_record_batch_rows = 1024,
        max_concurrent_closes = 4,
        enable_validate_compaction = false,
        enable_prefetch = false,
        auto_min_small_files_count = 5,
        auto_min_delete_heavy_files_count = 1,
        max_auto_plans_per_run = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        strategy: &str,
        target_file_size_bytes: u64,
        small_file_threshold_bytes: u64,
        min_size_per_partition: u64,
        max_file_count_per_partition: usize,
        max_input_parallelism: Option<usize>,
        max_output_parallelism: Option<usize>,
        grouping_strategy: &str,
        bin_pack_target_size_bytes: Option<u64>,
        min_group_size_bytes: Option<u64>,
        min_group_file_count: Option<usize>,
        min_delete_file_count_threshold: usize,
        data_file_prefix: &str,
        max_record_batch_rows: usize,
        max_concurrent_closes: usize,
        enable_validate_compaction: bool,
        enable_prefetch: bool,
        auto_min_small_files_count: usize,
        auto_min_delete_heavy_files_count: usize,
        max_auto_plans_per_run: Option<usize>,
    ) -> PyResult<Self> {
        let valid_strategies = ["auto", "small_files", "full", "files_with_deletes"];
        if !valid_strategies.contains(&strategy) {
            return Err(ConfigError::new_err(format!(
                "Invalid strategy '{strategy}'. Must be one of: {valid_strategies:?}"
            )));
        }
        let valid_grouping = ["single", "bin_pack"];
        if !valid_grouping.contains(&grouping_strategy) {
            return Err(ConfigError::new_err(format!(
                "Invalid grouping_strategy '{grouping_strategy}'. Must be one of: {valid_grouping:?}"
            )));
        }
        if let Some(n) = max_auto_plans_per_run
            && n == 0
        {
            return Err(ConfigError::new_err(
                "max_auto_plans_per_run must be greater than 0",
            ));
        }
        Ok(Self {
            strategy: strategy.to_owned(),
            target_file_size_bytes,
            small_file_threshold_bytes,
            min_size_per_partition,
            max_file_count_per_partition,
            max_input_parallelism,
            max_output_parallelism,
            grouping_strategy: grouping_strategy.to_owned(),
            bin_pack_target_size_bytes,
            min_group_size_bytes,
            min_group_file_count,
            min_delete_file_count_threshold,
            data_file_prefix: data_file_prefix.to_owned(),
            max_record_batch_rows,
            max_concurrent_closes,
            enable_validate_compaction,
            enable_prefetch,
            auto_min_small_files_count,
            auto_min_delete_heavy_files_count,
            max_auto_plans_per_run,
        })
    }

    fn __repr__(&self) -> String {
        format!("CompactionConfig(strategy='{}')", self.strategy)
    }
}

impl PyCompactionConfig {
    fn grouping_strategy(&self) -> GroupingStrategy {
        match self.grouping_strategy.as_str() {
            "bin_pack" => GroupingStrategy::BinPack(BinPackConfig::new(
                self.bin_pack_target_size_bytes
                    .unwrap_or(100 * 1024 * 1024 * 1024),
            )),
            _ => GroupingStrategy::Single,
        }
    }

    fn group_filters(&self) -> Option<GroupFilters> {
        if self.min_group_size_bytes.is_none() && self.min_group_file_count.is_none() {
            return None;
        }
        let mut b = GroupFiltersBuilder::default();
        if let Some(v) = self.min_group_size_bytes {
            b.min_group_size_bytes(v);
        }
        if let Some(v) = self.min_group_file_count {
            b.min_group_file_count(v);
        }
        b.build().ok()
    }

    fn execution_config(
        &self,
    ) -> PyResult<iceberg_compaction_core::config::CompactionExecutionConfig> {
        let mut b = CompactionExecutionConfigBuilder::default();
        b.target_file_size_bytes(self.target_file_size_bytes);
        b.data_file_prefix(self.data_file_prefix.clone());
        b.enable_validate_compaction(self.enable_validate_compaction);
        b.max_record_batch_rows(self.max_record_batch_rows);
        b.max_concurrent_closes(self.max_concurrent_closes);
        b.enable_prefetch(self.enable_prefetch);
        b.build().map_err(|e| ConfigError::new_err(e.to_string()))
    }

    /// Build a [`CompactionConfig`] for non-auto strategies.
    pub(crate) fn to_compaction_config(&self) -> PyResult<CompactionConfig> {
        let planning = match self.strategy.as_str() {
            "small_files" => {
                let mut b = SmallFilesConfigBuilder::default();
                b.target_file_size_bytes(self.target_file_size_bytes);
                b.small_file_threshold_bytes(self.small_file_threshold_bytes);
                b.min_size_per_partition(self.min_size_per_partition);
                b.max_file_count_per_partition(self.max_file_count_per_partition);
                if let Some(v) = self.max_input_parallelism {
                    b.max_input_parallelism(v);
                }
                if let Some(v) = self.max_output_parallelism {
                    b.max_output_parallelism(v);
                }
                b.grouping_strategy(self.grouping_strategy());
                if let Some(gf) = self.group_filters() {
                    b.group_filters(gf);
                }
                CompactionPlanningConfig::SmallFiles(
                    b.build().map_err(|e| ConfigError::new_err(e.to_string()))?,
                )
            }
            "full" => {
                let mut b = FullCompactionConfigBuilder::default();
                b.target_file_size_bytes(self.target_file_size_bytes);
                b.min_size_per_partition(self.min_size_per_partition);
                b.max_file_count_per_partition(self.max_file_count_per_partition);
                if let Some(v) = self.max_input_parallelism {
                    b.max_input_parallelism(v);
                }
                if let Some(v) = self.max_output_parallelism {
                    b.max_output_parallelism(v);
                }
                b.grouping_strategy(self.grouping_strategy());
                CompactionPlanningConfig::Full(
                    b.build().map_err(|e| ConfigError::new_err(e.to_string()))?,
                )
            }
            "files_with_deletes" => {
                let mut b = FilesWithDeletesConfigBuilder::default();
                b.target_file_size_bytes(self.target_file_size_bytes);
                b.min_size_per_partition(self.min_size_per_partition);
                b.max_file_count_per_partition(self.max_file_count_per_partition);
                if let Some(v) = self.max_input_parallelism {
                    b.max_input_parallelism(v);
                }
                if let Some(v) = self.max_output_parallelism {
                    b.max_output_parallelism(v);
                }
                b.grouping_strategy(self.grouping_strategy());
                b.min_delete_file_count_threshold(self.min_delete_file_count_threshold);
                if let Some(gf) = self.group_filters() {
                    b.group_filters(gf);
                }
                CompactionPlanningConfig::FilesWithDeletes(
                    b.build().map_err(|e| ConfigError::new_err(e.to_string()))?,
                )
            }
            other => {
                return Err(ConfigError::new_err(format!(
                    "strategy '{other}' is not supported here; use strategy='auto' for auto-selection"
                )));
            }
        };
        Ok(CompactionConfig::new(planning, self.execution_config()?))
    }

    /// Build an [`AutoCompactionConfig`] for the ``"auto"`` strategy.
    pub(crate) fn to_auto_config(&self) -> PyResult<AutoCompactionConfig> {
        let mut b = AutoCompactionConfigBuilder::default();
        b.thresholds(AutoThresholds {
            min_small_files_count: self.auto_min_small_files_count,
            min_delete_heavy_files_count: self.auto_min_delete_heavy_files_count,
        });
        b.target_file_size_bytes(self.target_file_size_bytes);
        b.small_file_threshold_bytes(self.small_file_threshold_bytes);
        b.min_size_per_partition(self.min_size_per_partition);
        b.max_file_count_per_partition(self.max_file_count_per_partition);
        if let Some(v) = self.max_input_parallelism {
            b.max_input_parallelism(v);
        }
        if let Some(v) = self.max_output_parallelism {
            b.max_output_parallelism(v);
        }
        b.grouping_strategy(self.grouping_strategy());
        if let Some(gf) = self.group_filters() {
            b.group_filters(gf);
        }
        b.min_delete_file_count_threshold(self.min_delete_file_count_threshold);
        if let Some(n) = self.max_auto_plans_per_run {
            b.max_auto_plans_per_run(NonZeroUsize::new(n).expect("validated > 0 in constructor"));
        }
        b.execution(self.execution_config()?);
        b.build().map_err(|e| ConfigError::new_err(e.to_string()))
    }
}

// ── Result types ─────────────────────────────────────────────────────────────

/// Result returned by a successful compaction run.
///
/// ``None`` is returned when no compaction was needed (no-op).
#[pyclass(name = "CompactionResult")]
pub struct PyCompactionResult {
    #[pyo3(get)]
    pub input_files_count: usize,
    #[pyo3(get)]
    pub output_files_count: usize,
    #[pyo3(get)]
    pub input_bytes_total: u64,
    #[pyo3(get)]
    pub output_bytes_total: u64,
    #[pyo3(get)]
    pub manifest_rewrite: Option<PyRewriteManifestsOpResult>,
}

#[pymethods]
impl PyCompactionResult {
    #[new]
    #[pyo3(signature = (input_files_count, output_files_count, input_bytes_total, output_bytes_total, manifest_rewrite = None))]
    fn new(
        input_files_count: usize,
        output_files_count: usize,
        input_bytes_total: u64,
        output_bytes_total: u64,
        manifest_rewrite: Option<PyRewriteManifestsOpResult>,
    ) -> Self {
        Self {
            input_files_count,
            output_files_count,
            input_bytes_total,
            output_bytes_total,
            manifest_rewrite,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "CompactionResult(input_files={}, output_files={}, \
             input_bytes={}, output_bytes={})",
            self.input_files_count,
            self.output_files_count,
            self.input_bytes_total,
            self.output_bytes_total,
        )
    }
}

impl From<iceberg_compaction_core::compaction::CompactionResult> for PyCompactionResult {
    fn from(r: iceberg_compaction_core::compaction::CompactionResult) -> Self {
        let manifest_rewrite = r.manifest_rewrite.map(|mr| PyRewriteManifestsOpResult {
            kept_manifests_count: mr.stats.kept_manifests_count,
            created_manifests_count: mr.stats.created_manifests_count,
            replaced_manifests_count: mr.stats.replaced_manifests_count,
            processed_entry_count: mr.stats.processed_entry_count,
        });
        Self {
            input_files_count: r.stats.input_files_count,
            output_files_count: r.stats.output_files_count,
            input_bytes_total: r.stats.input_total_bytes,
            output_bytes_total: r.stats.output_total_bytes,
            manifest_rewrite,
        }
    }
}

// ── Compaction operation ──────────────────────────────────────────────────────

enum InnerCompaction {
    Standard(Compaction),
    Auto(Box<AutoCompaction>),
}

/// Compact data files for an Iceberg table.
///
/// Use ``strategy="auto"`` (default) to let the library pick the best approach,
/// or choose a specific strategy. Returns ``None`` when no compaction was needed.
///
/// Example::
///
///     result = Compaction(
///         catalog=catalog,
///         table="my_namespace.my_table",
///         config=CompactionConfig(strategy="auto"),
///     ).run()
///     if result:
///         print(f"Compacted {result.input_files_count} → {result.output_files_count} files")
#[pyclass(name = "Compaction")]
pub struct PyCompaction {
    inner: InnerCompaction,
}

#[pymethods]
impl PyCompaction {
    #[new]
    #[pyo3(signature = (catalog, table, config, to_branch = "main", retry_config = None))]
    fn new(
        catalog: &Bound<'_, PyAny>,
        table: &str,
        config: &PyCompactionConfig,
        to_branch: &str,
        retry_config: Option<&PyRetryConfig>,
    ) -> PyResult<Self> {
        let cat = CatalogRef::from_py(catalog)?.inner();
        let ident = parse_table_ident(table)?;
        let retry = retry_config.map(|r| r.to_rust()).unwrap_or_default();

        let inner = if config.strategy == "auto" {
            InnerCompaction::Auto(Box::new(
                AutoCompactionBuilder::new(cat, ident, config.to_auto_config()?)
                    .with_to_branch(to_branch.to_owned())
                    .with_retry_config(retry)
                    .build(),
            ))
        } else {
            InnerCompaction::Standard(
                CompactionBuilder::new(cat, ident)
                    .with_config(Arc::new(config.to_compaction_config()?))
                    .with_to_branch(to_branch.to_owned())
                    .with_retry_config(retry)
                    .build(),
            )
        };
        Ok(Self { inner })
    }

    /// Execute the compaction. Returns a :class:`CompactionResult` or ``None`` if
    /// the table did not need compacting.
    fn run(&self, py: Python<'_>) -> PyResult<Option<PyCompactionResult>> {
        py.allow_threads(|| match &self.inner {
            InnerCompaction::Standard(c) => runtime().block_on(c.compact()),
            InnerCompaction::Auto(c) => runtime().block_on(c.compact()),
        })
        .map(|opt| opt.map(PyCompactionResult::from))
        .map_err(map_err)
    }

    fn __repr__(&self) -> String {
        "Compaction(...)".to_owned()
    }
}

// ── RewriteManifestsConfig (also used in manifest_rewrite.rs) ────────────────

/// Configuration for manifest file rewriting.
#[pyclass(name = "RewriteManifestsConfig")]
#[derive(Clone)]
pub struct PyRewriteManifestsConfig {
    #[pyo3(get)]
    pub clustering_strategy: String,
    #[pyo3(get)]
    pub snapshot_properties: HashMap<String, String>,
}

#[pymethods]
impl PyRewriteManifestsConfig {
    #[new]
    #[pyo3(signature = (clustering_strategy = "single", snapshot_properties = None))]
    fn new(
        clustering_strategy: &str,
        snapshot_properties: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        let valid = ["single", "by_partition_spec_id"];
        if !valid.contains(&clustering_strategy) {
            return Err(ConfigError::new_err(format!(
                "Invalid clustering_strategy '{clustering_strategy}'. Must be one of: {valid:?}"
            )));
        }
        Ok(Self {
            clustering_strategy: clustering_strategy.to_owned(),
            snapshot_properties: snapshot_properties.unwrap_or_default(),
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "RewriteManifestsConfig(clustering_strategy='{}')",
            self.clustering_strategy
        )
    }
}

impl PyRewriteManifestsConfig {
    pub(crate) fn to_rust(&self) -> RewriteManifestsConfig {
        let clustering_strategy = match self.clustering_strategy.as_str() {
            "by_partition_spec_id" => ManifestClusteringStrategy::ByPartitionSpecId,
            _ => ManifestClusteringStrategy::Single,
        };
        RewriteManifestsConfigBuilder::default()
            .clustering_strategy(clustering_strategy)
            .snapshot_properties(self.snapshot_properties.clone())
            .build()
            .expect("RewriteManifestsConfig should always build")
    }
}

use std::collections::HashMap;
