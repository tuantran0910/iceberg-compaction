use std::collections::HashMap;
use std::sync::Arc;

use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_glue::GlueCatalogBuilder;
use iceberg_catalog_hms::HmsCatalogBuilder;
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_catalog_s3tables::S3TablesCatalogBuilder;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;

use crate::runtime;

// ── Catalog pyclass types ────────────────────────────────────────────────────

/// In-memory catalog backed by the local filesystem. Intended for testing and
/// local development only.
#[pyclass]
pub struct MemoryCatalog {
    pub(crate) inner: Arc<dyn Catalog>,
}

/// REST-compatible Iceberg catalog (Polaris, Nessie, Tabular, BigLake, …).
#[pyclass]
pub struct RestCatalog {
    pub(crate) inner: Arc<dyn Catalog>,
}

/// AWS Glue Data Catalog.
#[pyclass]
pub struct GlueCatalog {
    pub(crate) inner: Arc<dyn Catalog>,
}

/// Apache Hive Metastore catalog (HMS) accessed over Thrift.
#[pyclass]
pub struct HmsCatalog {
    pub(crate) inner: Arc<dyn Catalog>,
}

/// AWS S3 Tables managed Iceberg catalog.
#[pyclass]
pub struct S3TablesCatalog {
    pub(crate) inner: Arc<dyn Catalog>,
}

// ── Python constructors ──────────────────────────────────────────────────────

#[pymethods]
impl MemoryCatalog {
    /// Create an in-memory catalog backed by a local directory.
    ///
    /// Args:
    ///     name: Catalog name.
    ///     warehouse: Local filesystem path for storing table metadata.
    #[new]
    fn new(name: &str, warehouse: &str) -> PyResult<Self> {
        let props = HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_owned(), warehouse.to_owned())]);
        let catalog = runtime()
            .block_on(MemoryCatalogBuilder::default().load(name, props))
            .map_err(|e| PyValueError::new_err(format!("Failed to create MemoryCatalog: {e}")))?;
        Ok(Self {
            inner: Arc::new(catalog),
        })
    }

    fn __repr__(&self) -> String {
        "MemoryCatalog(...)".to_owned()
    }
}

#[pymethods]
impl RestCatalog {
    /// Create a REST catalog.
    ///
    /// Args:
    ///     name: Catalog name.
    ///     uri: REST API endpoint URL (required).
    ///     warehouse: Warehouse location URI (optional).
    ///     properties: Additional properties such as auth tokens and custom
    ///         headers. Common keys:
    ///           "token"            – static Bearer token
    ///           "credential"       – OAuth2 "client_id:secret" or "secret"
    ///           "oauth2-server-uri"– custom token endpoint
    ///           "header.<name>"    – custom HTTP request header
    ///           "google-auth"      – set to "true" to use Google ADC (Application
    ///                               Default Credentials) for BigLake Metastore
    #[new]
    #[pyo3(signature = (name, uri, warehouse = None, properties = None))]
    fn new(
        name: &str,
        uri: &str,
        warehouse: Option<&str>,
        properties: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        let mut props = properties.unwrap_or_default();
        props.insert("uri".to_owned(), uri.to_owned());
        if let Some(wh) = warehouse {
            props.insert("warehouse".to_owned(), wh.to_owned());
        }
        let catalog = runtime()
            .block_on(RestCatalogBuilder::default().load(name, props))
            .map_err(|e| PyValueError::new_err(format!("Failed to create RestCatalog: {e}")))?;
        Ok(Self {
            inner: Arc::new(catalog),
        })
    }

    fn __repr__(&self) -> String {
        "RestCatalog(...)".to_owned()
    }
}

#[pymethods]
impl GlueCatalog {
    /// Create an AWS Glue Data Catalog.
    ///
    /// Args:
    ///     name: Catalog name.
    ///     warehouse: S3 warehouse location (required).
    ///     properties: Additional properties. Common keys:
    ///           "catalog_id"         – AWS Glue catalog ID (account ID)
    ///           "region"             – AWS region
    ///           "profile_name"       – AWS profile name
    ///           "aws_access_key_id"  – explicit access key
    ///           "aws_secret_access_key" – explicit secret key
    ///           "aws_session_token"  – session token
    #[new]
    #[pyo3(signature = (name, warehouse, properties = None))]
    fn new(
        name: &str,
        warehouse: &str,
        properties: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        let mut props = properties.unwrap_or_default();
        props.insert("warehouse".to_owned(), warehouse.to_owned());
        let catalog = runtime()
            .block_on(GlueCatalogBuilder::default().load(name, props))
            .map_err(|e| PyValueError::new_err(format!("Failed to create GlueCatalog: {e}")))?;
        Ok(Self {
            inner: Arc::new(catalog),
        })
    }

    fn __repr__(&self) -> String {
        "GlueCatalog(...)".to_owned()
    }
}

#[pymethods]
impl HmsCatalog {
    /// Create a Hive Metastore (HMS) catalog.
    ///
    /// Args:
    ///     name: Catalog name.
    ///     uri: HMS Thrift address as "host:port" (required).
    ///     warehouse: Warehouse location URI (required).
    ///     properties: Additional properties. Common keys:
    ///           "thrift_transport" – "buffered" (default) or "framed"
    #[new]
    #[pyo3(signature = (name, uri, warehouse, properties = None))]
    fn new(
        name: &str,
        uri: &str,
        warehouse: &str,
        properties: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        let mut props = properties.unwrap_or_default();
        props.insert("uri".to_owned(), uri.to_owned());
        props.insert("warehouse".to_owned(), warehouse.to_owned());
        let catalog = runtime()
            .block_on(HmsCatalogBuilder::default().load(name, props))
            .map_err(|e| PyValueError::new_err(format!("Failed to create HmsCatalog: {e}")))?;
        Ok(Self {
            inner: Arc::new(catalog),
        })
    }

    fn __repr__(&self) -> String {
        "HmsCatalog(...)".to_owned()
    }
}

#[pymethods]
impl S3TablesCatalog {
    /// Create an AWS S3 Tables catalog.
    ///
    /// Args:
    ///     name: Catalog name.
    ///     table_bucket_arn: S3 Tables bucket ARN, e.g.
    ///         "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket".
    ///     properties: Additional properties. Common keys:
    ///           "endpoint_url"         – custom endpoint (LocalStack, etc.)
    ///           "region_name"          – AWS region
    ///           "aws_access_key_id"    – explicit access key
    ///           "aws_secret_access_key"– explicit secret key
    ///           "aws_session_token"    – session token
    #[new]
    #[pyo3(signature = (name, table_bucket_arn, properties = None))]
    fn new(
        name: &str,
        table_bucket_arn: &str,
        properties: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        let mut props = properties.unwrap_or_default();
        props.insert("table_bucket_arn".to_owned(), table_bucket_arn.to_owned());
        let catalog = runtime()
            .block_on(S3TablesCatalogBuilder::default().load(name, props))
            .map_err(|e| PyValueError::new_err(format!("Failed to create S3TablesCatalog: {e}")))?;
        Ok(Self {
            inner: Arc::new(catalog),
        })
    }

    fn __repr__(&self) -> String {
        "S3TablesCatalog(...)".to_owned()
    }
}

// ── Internal CatalogRef ──────────────────────────────────────────────────────

/// Internal enum that holds an `Arc<dyn Catalog>` extracted from any Python
/// catalog class. Used by operation constructors to accept any catalog type.
pub(crate) enum CatalogRef {
    Memory(Arc<dyn Catalog>),
    Rest(Arc<dyn Catalog>),
    Glue(Arc<dyn Catalog>),
    Hms(Arc<dyn Catalog>),
    S3Tables(Arc<dyn Catalog>),
}

impl CatalogRef {
    /// Downcast a Python object to one of the known catalog types and extract
    /// its `Arc<dyn Catalog>`. Returns `PyTypeError` if the object is not a
    /// recognized catalog.
    pub fn from_py(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(c) = obj.extract::<PyRef<MemoryCatalog>>() {
            return Ok(Self::Memory(c.inner.clone()));
        }
        if let Ok(c) = obj.extract::<PyRef<RestCatalog>>() {
            return Ok(Self::Rest(c.inner.clone()));
        }
        if let Ok(c) = obj.extract::<PyRef<GlueCatalog>>() {
            return Ok(Self::Glue(c.inner.clone()));
        }
        if let Ok(c) = obj.extract::<PyRef<HmsCatalog>>() {
            return Ok(Self::Hms(c.inner.clone()));
        }
        if let Ok(c) = obj.extract::<PyRef<S3TablesCatalog>>() {
            return Ok(Self::S3Tables(c.inner.clone()));
        }
        Err(PyTypeError::new_err(
            "Expected a catalog instance: MemoryCatalog, RestCatalog, GlueCatalog, \
             HmsCatalog, or S3TablesCatalog",
        ))
    }

    pub fn inner(&self) -> Arc<dyn Catalog> {
        match self {
            Self::Memory(c) | Self::Rest(c) | Self::Glue(c) | Self::Hms(c) | Self::S3Tables(c) => {
                c.clone()
            }
        }
    }
}

// ── TableIdent helper ────────────────────────────────────────────────────────

/// Parse a dot-separated table identifier string into an iceberg `TableIdent`.
///
/// The last segment is the table name; preceding segments form the namespace.
/// Examples:
///   "my_namespace.my_table" → namespace=["my_namespace"], name="my_table"
///   "db.schema.table"       → namespace=["db","schema"],  name="table"
pub(crate) fn parse_table_ident(s: &str) -> PyResult<TableIdent> {
    TableIdent::from_strs(s.split('.'))
        .map_err(|e| PyValueError::new_err(format!("Invalid table identifier '{s}': {e}")))
}

#[allow(dead_code)]
pub(crate) fn make_namespace(parts: Vec<&str>) -> PyResult<NamespaceIdent> {
    NamespaceIdent::from_vec(parts.into_iter().map(str::to_owned).collect())
        .map_err(|e| PyValueError::new_err(format!("Invalid namespace: {e}")))
}
