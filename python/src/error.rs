use iceberg::ErrorKind;
use iceberg_compaction_core::CompactionError;
use pyo3::exceptions::PyException;
use pyo3::{PyErr, create_exception};

create_exception!(
    iceberg_compaction,
    IcebergCompactionError,
    PyException,
    "Base exception for all iceberg-compaction errors."
);
create_exception!(
    iceberg_compaction,
    CatalogError,
    IcebergCompactionError,
    "Raised when a catalog operation fails."
);
create_exception!(
    iceberg_compaction,
    CommitConflictError,
    IcebergCompactionError,
    "Raised when a commit is rejected due to a concurrent modification."
);
create_exception!(
    iceberg_compaction,
    FeatureUnsupportedError,
    IcebergCompactionError,
    "Raised when the operation is not supported for the table format version."
);
create_exception!(
    iceberg_compaction,
    ConfigError,
    IcebergCompactionError,
    "Raised when a configuration value is invalid."
);

/// Map a [`CompactionError`] to the appropriate Python exception.
pub(crate) fn map_err(e: CompactionError) -> PyErr {
    match &e {
        CompactionError::Config(_) => ConfigError::new_err(e.to_string()),
        CompactionError::Iceberg(ie) => match ie.kind() {
            ErrorKind::CatalogCommitConflicts => CommitConflictError::new_err(e.to_string()),
            ErrorKind::FeatureUnsupported => FeatureUnsupportedError::new_err(e.to_string()),
            _ => CatalogError::new_err(e.to_string()),
        },
        _ => IcebergCompactionError::new_err(e.to_string()),
    }
}
