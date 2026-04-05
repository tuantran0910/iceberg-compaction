use iceberg_compaction_core::RewriteManifests;
use iceberg_compaction_core::manifest_rewrite::RewriteManifestsBuilder;
use pyo3::prelude::*;

use crate::catalog::{CatalogRef, parse_table_ident};
use crate::compaction::{PyRetryConfig, PyRewriteManifestsConfig};
use crate::error::map_err;
use crate::runtime;

// ── Result type ───────────────────────────────────────────────────────────────

/// Result returned by a successful manifest rewrite run.
///
/// ``None`` is returned when no rewrite was needed (no-op).
#[pyclass(name = "RewriteManifestsResult")]
#[derive(Clone)]
pub struct PyRewriteManifestsOpResult {
    #[pyo3(get)]
    pub kept_manifests_count: u64,
    #[pyo3(get)]
    pub created_manifests_count: u64,
    #[pyo3(get)]
    pub replaced_manifests_count: u64,
    #[pyo3(get)]
    pub processed_entry_count: u64,
}

#[pymethods]
impl PyRewriteManifestsOpResult {
    #[new]
    fn new(
        kept_manifests_count: u64,
        created_manifests_count: u64,
        replaced_manifests_count: u64,
        processed_entry_count: u64,
    ) -> Self {
        Self {
            kept_manifests_count,
            created_manifests_count,
            replaced_manifests_count,
            processed_entry_count,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "RewriteManifestsResult(kept={}, created={}, replaced={}, entries={})",
            self.kept_manifests_count,
            self.created_manifests_count,
            self.replaced_manifests_count,
            self.processed_entry_count,
        )
    }
}

impl From<iceberg_compaction_core::manifest_rewrite::RewriteManifestsResult>
    for PyRewriteManifestsOpResult
{
    fn from(r: iceberg_compaction_core::manifest_rewrite::RewriteManifestsResult) -> Self {
        Self {
            kept_manifests_count: r.stats.kept_manifests_count,
            created_manifests_count: r.stats.created_manifests_count,
            replaced_manifests_count: r.stats.replaced_manifests_count,
            processed_entry_count: r.stats.processed_entry_count,
        }
    }
}

// ── Operation ────────────────────────────────────────────────────────────────

/// Rewrite manifest files for an Iceberg table.
///
/// This is a metadata-only operation — no data files are read or written.
/// Returns ``None`` when no rewrite was needed (no-op).
///
/// Example::
///
///     result = RewriteManifests(
///         catalog=catalog,
///         table="my_namespace.my_table",
///         config=RewriteManifestsConfig(),
///     ).run()
#[pyclass(name = "RewriteManifests")]
pub struct PyRewriteManifests {
    inner: RewriteManifests,
}

#[pymethods]
impl PyRewriteManifests {
    #[new]
    #[pyo3(signature = (catalog, table, config, to_branch = "main", retry_config = None))]
    fn new(
        catalog: &Bound<'_, PyAny>,
        table: &str,
        config: &PyRewriteManifestsConfig,
        to_branch: &str,
        retry_config: Option<&PyRetryConfig>,
    ) -> PyResult<Self> {
        let cat = CatalogRef::from_py(catalog)?.inner();
        let ident = parse_table_ident(table)?;
        let retry = retry_config.map(|r| r.to_rust()).unwrap_or_default();
        let inner = RewriteManifestsBuilder::new(cat, ident, config.to_rust())
            .with_to_branch(to_branch.to_owned())
            .with_retry_config(retry)
            .build();
        Ok(Self { inner })
    }

    /// Execute the manifest rewrite. Returns a :class:`RewriteManifestsResult`
    /// or ``None`` if no rewrite was needed.
    fn run(&self, py: Python<'_>) -> PyResult<Option<PyRewriteManifestsOpResult>> {
        py.allow_threads(|| runtime().block_on(self.inner.rewrite()))
            .map(|opt| opt.map(PyRewriteManifestsOpResult::from))
            .map_err(map_err)
    }

    fn __repr__(&self) -> String {
        "RewriteManifests(...)".to_owned()
    }
}
