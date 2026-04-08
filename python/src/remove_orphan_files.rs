use std::time::Duration;

use iceberg_compaction_core::RemoveOrphanFiles;
use iceberg_compaction_core::config::RemoveOrphanFilesConfigBuilder;
use iceberg_compaction_core::remove_orphan_files::RemoveOrphanFilesBuilder;
use pyo3::prelude::*;

use crate::catalog::{CatalogRef, parse_table_ident};
use crate::error::{ConfigError, map_err};
use crate::runtime;

// ── Config ────────────────────────────────────────────────────────────────────

/// Configuration for orphan file removal.
#[pyclass(name = "RemoveOrphanFilesConfig")]
#[derive(Clone)]
pub struct PyRemoveOrphanFilesConfig {
    #[pyo3(get)]
    pub older_than_secs: u64,
    #[pyo3(get)]
    pub dry_run: bool,
    #[pyo3(get)]
    pub load_concurrency: usize,
    #[pyo3(get)]
    pub delete_concurrency: usize,
}

#[pymethods]
impl PyRemoveOrphanFilesConfig {
    #[new]
    #[pyo3(signature = (
        older_than_secs = 604800,
        dry_run = false,
        load_concurrency = 16,
        delete_concurrency = 10,
    ))]
    fn new(
        older_than_secs: u64,
        dry_run: bool,
        load_concurrency: usize,
        delete_concurrency: usize,
    ) -> Self {
        Self {
            older_than_secs,
            dry_run,
            load_concurrency,
            delete_concurrency,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "RemoveOrphanFilesConfig(older_than_secs={}, dry_run={})",
            self.older_than_secs,
            if self.dry_run { "True" } else { "False" }
        )
    }
}

impl PyRemoveOrphanFilesConfig {
    pub(crate) fn to_rust(
        &self,
    ) -> PyResult<iceberg_compaction_core::config::RemoveOrphanFilesConfig> {
        RemoveOrphanFilesConfigBuilder::default()
            .older_than(Duration::from_secs(self.older_than_secs))
            .dry_run(self.dry_run)
            .load_concurrency(self.load_concurrency)
            .delete_concurrency(self.delete_concurrency)
            .build()
            .map_err(|e| ConfigError::new_err(e.to_string()))
    }
}

// ── Result type ───────────────────────────────────────────────────────────────

/// Result returned by an orphan file removal run.
///
/// Unlike other operations, this always returns a result (never ``None``).
#[pyclass(name = "RemoveOrphanFilesResult")]
pub struct PyRemoveOrphanFilesResult {
    #[pyo3(get)]
    pub orphan_file_paths: Vec<String>,
    #[pyo3(get)]
    pub orphan_files_found: u64,
    #[pyo3(get)]
    pub orphan_files_deleted: u64,
}

#[pymethods]
impl PyRemoveOrphanFilesResult {
    #[new]
    fn new(
        orphan_file_paths: Vec<String>,
        orphan_files_found: u64,
        orphan_files_deleted: u64,
    ) -> Self {
        Self {
            orphan_file_paths,
            orphan_files_found,
            orphan_files_deleted,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "RemoveOrphanFilesResult(found={}, deleted={})",
            self.orphan_files_found, self.orphan_files_deleted
        )
    }
}

impl From<iceberg_compaction_core::remove_orphan_files::RemoveOrphanFilesResult>
    for PyRemoveOrphanFilesResult
{
    fn from(r: iceberg_compaction_core::remove_orphan_files::RemoveOrphanFilesResult) -> Self {
        Self {
            orphan_files_found: r.stats.orphan_files_found,
            orphan_files_deleted: r.stats.orphan_files_deleted,
            orphan_file_paths: r.orphan_file_paths,
        }
    }
}

// ── Operation ────────────────────────────────────────────────────────────────

/// Remove orphan files from an Iceberg table.
///
/// Files under the table location that are not referenced by any snapshot or
/// table metadata and are older than ``older_than_secs`` are deleted. Use
/// ``dry_run=True`` to identify orphans without deleting them.
///
/// This operation always returns a :class:`RemoveOrphanFilesResult` (never ``None``).
///
/// Example::
///
///     result = RemoveOrphanFiles(
///         catalog=catalog,
///         table="my_namespace.my_table",
///         config=RemoveOrphanFilesConfig(dry_run=True),
///     ).run()
///     print(f"Found {result.orphan_files_found} orphan files")
#[pyclass(name = "RemoveOrphanFiles")]
pub struct PyRemoveOrphanFiles {
    inner: RemoveOrphanFiles,
}

#[pymethods]
impl PyRemoveOrphanFiles {
    #[new]
    #[pyo3(signature = (catalog, table, config))]
    fn new(
        catalog: &Bound<'_, PyAny>,
        table: &str,
        config: &PyRemoveOrphanFilesConfig,
    ) -> PyResult<Self> {
        let cat = CatalogRef::from_py(catalog)?.inner();
        let ident = parse_table_ident(table)?;
        let inner = RemoveOrphanFilesBuilder::new(cat, ident, config.to_rust()?).build();
        Ok(Self { inner })
    }

    /// Execute orphan file removal. Always returns a :class:`RemoveOrphanFilesResult`.
    fn run(&self, py: Python<'_>) -> PyResult<PyRemoveOrphanFilesResult> {
        py.allow_threads(|| runtime().block_on(self.inner.execute()))
            .map(PyRemoveOrphanFilesResult::from)
            .map_err(map_err)
    }

    fn __repr__(&self) -> String {
        "RemoveOrphanFiles(...)".to_owned()
    }
}
