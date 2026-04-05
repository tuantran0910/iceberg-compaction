use std::time::Duration;

use iceberg_compaction_core::ExpireSnapshots;
use iceberg_compaction_core::config::ExpireSnapshotsConfigBuilder;
use iceberg_compaction_core::expire_snapshots::ExpireSnapshotsBuilder;
use pyo3::prelude::*;

use crate::catalog::{CatalogRef, parse_table_ident};
use crate::compaction::PyRetryConfig;
use crate::error::{ConfigError, map_err};
use crate::runtime;

// ── Config ────────────────────────────────────────────────────────────────────

/// Configuration for snapshot expiration.
#[pyclass(name = "ExpireSnapshotsConfig")]
#[derive(Clone)]
pub struct PyExpireSnapshotsConfig {
    #[pyo3(get)]
    pub retain_last: i32,
    #[pyo3(get)]
    pub max_snapshot_age_secs: u64,
    #[pyo3(get)]
    pub snapshot_ids_to_expire: Vec<i64>,
    #[pyo3(get)]
    pub clear_expired_metadata: bool,
}

#[pymethods]
impl PyExpireSnapshotsConfig {
    #[new]
    #[pyo3(signature = (
        retain_last = 1,
        max_snapshot_age_secs = 432000,
        snapshot_ids_to_expire = None,
        clear_expired_metadata = false,
    ))]
    fn new(
        retain_last: i32,
        max_snapshot_age_secs: u64,
        snapshot_ids_to_expire: Option<Vec<i64>>,
        clear_expired_metadata: bool,
    ) -> Self {
        Self {
            retain_last,
            max_snapshot_age_secs,
            snapshot_ids_to_expire: snapshot_ids_to_expire.unwrap_or_default(),
            clear_expired_metadata,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "ExpireSnapshotsConfig(retain_last={}, max_snapshot_age_secs={})",
            self.retain_last, self.max_snapshot_age_secs
        )
    }
}

impl PyExpireSnapshotsConfig {
    pub(crate) fn to_rust(
        &self,
    ) -> PyResult<iceberg_compaction_core::config::ExpireSnapshotsConfig> {
        ExpireSnapshotsConfigBuilder::default()
            .retain_last(self.retain_last)
            .max_snapshot_age(Duration::from_secs(self.max_snapshot_age_secs))
            .snapshot_ids_to_expire(self.snapshot_ids_to_expire.clone())
            .clear_expired_metadata(self.clear_expired_metadata)
            .build()
            .map_err(|e| ConfigError::new_err(e.to_string()))
    }
}

// ── Result type ───────────────────────────────────────────────────────────────

/// Result returned by a successful snapshot expiration run.
///
/// ``None`` is returned when no snapshots were eligible for expiration.
#[pyclass(name = "ExpireSnapshotsResult")]
pub struct PyExpireSnapshotsResult {
    #[pyo3(get)]
    pub expired_snapshot_count: u64,
}

#[pymethods]
impl PyExpireSnapshotsResult {
    #[new]
    fn new(expired_snapshot_count: u64) -> Self {
        Self {
            expired_snapshot_count,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "ExpireSnapshotsResult(expired_snapshot_count={})",
            self.expired_snapshot_count
        )
    }
}

impl From<iceberg_compaction_core::expire_snapshots::ExpireSnapshotsResult>
    for PyExpireSnapshotsResult
{
    fn from(r: iceberg_compaction_core::expire_snapshots::ExpireSnapshotsResult) -> Self {
        Self {
            expired_snapshot_count: r.stats.expired_snapshot_count,
        }
    }
}

// ── Operation ────────────────────────────────────────────────────────────────

/// Expire old snapshots from an Iceberg table.
///
/// Returns ``None`` when no snapshots were eligible for expiration (no-op).
///
/// Example::
///
///     result = ExpireSnapshots(
///         catalog=catalog,
///         table="my_namespace.my_table",
///         config=ExpireSnapshotsConfig(retain_last=5),
///     ).run()
#[pyclass(name = "ExpireSnapshots")]
pub struct PyExpireSnapshots {
    inner: ExpireSnapshots,
}

#[pymethods]
impl PyExpireSnapshots {
    #[new]
    #[pyo3(signature = (catalog, table, config, to_branch = "main", retry_config = None))]
    fn new(
        catalog: &Bound<'_, PyAny>,
        table: &str,
        config: &PyExpireSnapshotsConfig,
        to_branch: &str,
        retry_config: Option<&PyRetryConfig>,
    ) -> PyResult<Self> {
        let cat = CatalogRef::from_py(catalog)?.inner();
        let ident = parse_table_ident(table)?;
        let retry = retry_config.map(|r| r.to_rust()).unwrap_or_default();
        let inner = ExpireSnapshotsBuilder::new(cat, ident, config.to_rust()?)
            .with_to_branch(to_branch.to_owned())
            .with_retry_config(retry)
            .build();
        Ok(Self { inner })
    }

    /// Execute the snapshot expiration. Returns an :class:`ExpireSnapshotsResult`
    /// or ``None`` if no expiration was needed.
    fn run(&self, py: Python<'_>) -> PyResult<Option<PyExpireSnapshotsResult>> {
        py.allow_threads(|| runtime().block_on(self.inner.expire()))
            .map(|opt| opt.map(PyExpireSnapshotsResult::from))
            .map_err(map_err)
    }

    fn __repr__(&self) -> String {
        "ExpireSnapshots(...)".to_owned()
    }
}
