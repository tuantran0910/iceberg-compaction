//! Python bindings for iceberg-compaction.
//!
//! This module exposes the core compaction operations to Python via PyO3.
//! The `_internal` name is intentional: import this crate as `from iceberg_compaction import _internal`.
//!
//! # Architecture
//!
//! All public types live under the `_internal` module to signal that this
//! is not a public API with stability guarantees. The module provides catalog
//! wrappers, operation builders, and result types for the four maintenance operations.

use std::sync::OnceLock;

use pyo3::prelude::*;

mod catalog;
mod compaction;
mod error;
mod expire_snapshots;
mod manifest_rewrite;
mod remove_orphan_files;

use catalog::{GlueCatalog, HmsCatalog, MemoryCatalog, RestCatalog, S3TablesCatalog};
use compaction::{
    PyCompaction, PyCompactionConfig, PyCompactionResult, PyRetryConfig, PyRewriteManifestsConfig,
};
use error::{
    CatalogError, CommitConflictError, ConfigError, FeatureUnsupportedError, IcebergCompactionError,
};
use expire_snapshots::{PyExpireSnapshots, PyExpireSnapshotsConfig, PyExpireSnapshotsResult};
use manifest_rewrite::{PyRewriteManifests, PyRewriteManifestsOpResult};
use remove_orphan_files::{
    PyRemoveOrphanFiles, PyRemoveOrphanFilesConfig, PyRemoveOrphanFilesResult,
};

// ── Shared Tokio runtime ─────────────────────────────────────────────────────

static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

pub(crate) fn runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .inspect_err(|e| {
                eprintln!(
                    "Failed to create Tokio multi-threaded runtime, falling back to single-threaded: {e}"
                );
            })
            .unwrap_or_else(|_| {
                tokio::runtime::Builder::new_current_thread()
                    .build()
                    .expect("Failed to create fallback single-threaded Tokio runtime")
            })
    })
}

// ── Module definition ────────────────────────────────────────────────────────

#[pymodule]
fn _internal(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Catalogs
    m.add_class::<MemoryCatalog>()?;
    m.add_class::<RestCatalog>()?;
    m.add_class::<GlueCatalog>()?;
    m.add_class::<HmsCatalog>()?;
    m.add_class::<S3TablesCatalog>()?;

    // Configs
    m.add_class::<PyCompactionConfig>()?;
    m.add_class::<PyRewriteManifestsConfig>()?;
    m.add_class::<PyExpireSnapshotsConfig>()?;
    m.add_class::<PyRemoveOrphanFilesConfig>()?;
    m.add_class::<PyRetryConfig>()?;

    // Operations
    m.add_class::<PyCompaction>()?;
    m.add_class::<PyRewriteManifests>()?;
    m.add_class::<PyExpireSnapshots>()?;
    m.add_class::<PyRemoveOrphanFiles>()?;

    // Results
    m.add_class::<PyCompactionResult>()?;
    m.add_class::<PyRewriteManifestsOpResult>()?;
    m.add_class::<PyExpireSnapshotsResult>()?;
    m.add_class::<PyRemoveOrphanFilesResult>()?;

    // Exceptions
    m.add(
        "IcebergCompactionError",
        py.get_type::<IcebergCompactionError>(),
    )?;
    m.add("CatalogError", py.get_type::<CatalogError>())?;
    m.add("CommitConflictError", py.get_type::<CommitConflictError>())?;
    m.add(
        "FeatureUnsupportedError",
        py.get_type::<FeatureUnsupportedError>(),
    )?;
    m.add("ConfigError", py.get_type::<ConfigError>())?;

    Ok(())
}
