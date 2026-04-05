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

//! BigLake Metastore end-to-end table maintenance example.
//!
//! Demonstrates the full maintenance lifecycle on a BigLake Metastore Iceberg
//! REST catalog backed by GCS:
//!
//! 1. Connect to the BigLake REST catalog using Google Application Default Credentials (ADC)
//! 2. Create namespace if needed, drop table if exists, then create fresh
//! 3. Append several batches of synthetic data to build up multiple small files and snapshots
//! 4. Run compaction  — merge small files and apply any pending deletes
//! 5. Rewrite manifests — consolidate the manifest list
//! 6. Expire snapshots  — remove old snapshot metadata
//! 7. Remove orphan files — reclaim storage from unreferenced files
//!
//! # Prerequisites
//!
//! - Google ADC configured (`gcloud auth application-default login` or a service account
//!   key via `GOOGLE_APPLICATION_CREDENTIALS`)
//! - The service account must have the BigQuery Data Editor and Storage Object Admin
//!   roles on the project and GCS bucket
#![allow(clippy::doc_markdown)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Int32Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::{GCS_DISABLE_VM_METADATA, GCS_PROJECT_ID, GCS_USER_PROJECT};
use iceberg::spec::{
    DataFileFormat, MAIN_BRANCH, Manifest, NestedField, PrimitiveType, Schema, Type,
};
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
use iceberg_compaction_core::compaction::CompactionBuilder;
use iceberg_compaction_core::config::{
    CompactionConfigBuilder, CompactionPlanningConfig, SmallFilesConfig,
};
use iceberg_compaction_core::iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_compaction_core::{
    ExpireSnapshotsBuilder, ExpireSnapshotsConfig, RemoveOrphanFilesBuilder,
    RemoveOrphanFilesConfig, RewriteManifestsBuilder, RewriteManifestsConfig,
};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

// ── Project configuration ─────────────────────────────────────────────────────

const GCP_PROJECT_ID: &str = "rainbow-data-production-483609";
const CATALOG_NAME: &str = "rainbow-data-production-iceberg";
const CATALOG_URI: &str = "https://biglake.googleapis.com/iceberg/v1/restcatalog";
const WAREHOUSE_PATH: &str = "bq://projects/rainbow-data-production-483609";
const GCS_BUCKET_LOCATION: &str = "gs://rainbow-data-production-iceberg/test_iceberg_compactor";

// ── Table configuration ───────────────────────────────────────────────────────

const NAMESPACE: &str = "test_iceberg_compactor";
const TABLE_NAME: &str = "compaction_demo";

/// Number of small-file append batches to create before compaction.
///
/// Each batch produces one snapshot and one data file, giving the compactor
/// enough work to demonstrate meaningful before/after differences.
const APPEND_BATCHES: usize = 8;

/// Rows written per append batch.
const ROWS_PER_BATCH: usize = 500;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // ── 1. Build the BigLake REST catalog with Google ADC ─────────────────────

    println!("Building BigLake REST catalog with google-auth=true (ADC) …");

    let mut catalog_props: HashMap<String, String> = HashMap::new();

    // BigLake REST endpoint
    catalog_props.insert("uri".to_owned(), CATALOG_URI.to_owned());
    catalog_props.insert("warehouse".to_owned(), WAREHOUSE_PATH.to_owned());

    // Enable Google ADC authentication
    catalog_props.insert("google-auth".to_owned(), "true".to_owned());

    // Required request headers for BigLake
    catalog_props.insert(
        "header.x-goog-user-project".to_owned(),
        GCP_PROJECT_ID.to_owned(),
    );
    catalog_props.insert(
        "header.metrics.reporting-enabled".to_owned(),
        "false".to_owned(),
    );

    // GCS FileIO configuration
    catalog_props.insert(GCS_PROJECT_ID.to_owned(), GCP_PROJECT_ID.to_owned());
    catalog_props.insert(GCS_USER_PROJECT.to_owned(), GCP_PROJECT_ID.to_owned());
    catalog_props.insert(GCS_DISABLE_VM_METADATA.to_owned(), "false".to_owned());

    println!("  ✓ google-auth=true configured, RestCatalogBuilder handles ADC internally");

    let catalog: Arc<dyn Catalog> = Arc::new(
        RestCatalogBuilder::default()
            .load(CATALOG_NAME, catalog_props)
            .await
            .expect("failed to build BigLake REST catalog"),
    );
    println!("  ✓ Connected to BigLake catalog '{CATALOG_NAME}'");

    // ── 3. Setup namespace and table (drop if exists, create fresh) ────────────

    let namespace_ident = NamespaceIdent::new(NAMESPACE.to_owned());
    let table_ident = TableIdent::new(namespace_ident.clone(), TABLE_NAME.to_owned());

    setup_namespace(&*catalog, &namespace_ident).await?;
    let table = setup_table(&*catalog, &namespace_ident, &table_ident).await?;
    println!("  ✓ Table ready: {NAMESPACE}.{TABLE_NAME}");

    // ── 4. Append multiple small data batches ─────────────────────────────────

    println!("\nAppending {APPEND_BATCHES} small data batches …");
    // Use a unique run prefix so file names never clash with previous runs.
    let run_id = Uuid::new_v4().to_string();
    let run_id = &run_id[..8];
    let mut current_table = table;
    for i in 0..APPEND_BATCHES {
        current_table = append_batch(
            &*catalog,
            &current_table,
            i as i32 * ROWS_PER_BATCH as i32,
            run_id,
            i,
        )
        .await?;
        println!("  Batch {}/{APPEND_BATCHES} committed", i + 1);
    }

    let snapshot_count_before = current_table.metadata().snapshots().count();
    println!("  ✓ {APPEND_BATCHES} batches written — {snapshot_count_before} snapshots on table");

    // ── 5. Compaction: merge small files ─────────────────────────────────────

    println!("\n── Step 1: Compaction (rewrite data files) ──");
    let compaction_config = CompactionConfigBuilder::default()
        .planning(CompactionPlanningConfig::SmallFiles(
            SmallFilesConfig::default(),
        ))
        .build()?;

    let compaction = CompactionBuilder::new(catalog.clone(), table_ident.clone())
        .with_config(Arc::new(compaction_config))
        .with_catalog_name(CATALOG_NAME.to_owned())
        .build();

    let table_after_compact = match compaction.compact().await? {
        Some(result) => {
            let s = &result.stats;
            println!(
                "  ✓ Compacted {} → {} files  ({} → {} bytes)",
                s.input_files_count,
                s.output_files_count,
                s.input_total_bytes,
                s.output_total_bytes
            );
            result.table.expect("compaction result must include table")
        }
        None => {
            println!("  ✓ No compaction needed");
            catalog.load_table(&table_ident).await?
        }
    };

    // ── 5b. Verify data integrity after compaction ────────────────────────────

    let expected_rows: u64 = (APPEND_BATCHES * ROWS_PER_BATCH) as u64;
    let actual_rows = count_rows_via_manifest(&table_after_compact).await?;
    if actual_rows == expected_rows {
        println!("  ✓ Data integrity check passed: {actual_rows} rows");
    } else {
        return Err(format!(
            "Data integrity check FAILED: expected {expected_rows} rows, got {actual_rows}"
        )
        .into());
    }

    // ── 6. Rewrite manifests ───────────────────────────────────────────────────

    println!("\n── Step 2: Rewrite manifests ──");
    let manifest_rewriter = RewriteManifestsBuilder::new(
        catalog.clone(),
        table_ident.clone(),
        RewriteManifestsConfig::default(),
    )
    .with_to_branch(MAIN_BRANCH)
    .build();

    match manifest_rewriter.rewrite().await? {
        Some(result) => {
            let s = &result.stats;
            println!(
                "  ✓ Rewrote manifests: {} created, {} replaced ({} entries processed)",
                s.created_manifests_count, s.replaced_manifests_count, s.processed_entry_count
            );
        }
        None => println!("  ✓ Manifests already consolidated — no-op"),
    }

    // ── 7. Expire snapshots ────────────────────────────────────────────────────

    println!("\n── Step 3: Expire snapshots ──");
    let expire_config = ExpireSnapshotsConfig {
        // Keep the 2 most recent snapshots; expire everything older than 1 minute
        // (suitable for this demo — use longer durations in production).
        retain_last: 2,
        max_snapshot_age: Duration::from_secs(60),
        ..Default::default()
    };

    let expirer = ExpireSnapshotsBuilder::new(catalog.clone(), table_ident.clone(), expire_config)
        .with_to_branch(MAIN_BRANCH)
        .build();

    match expirer.expire().await? {
        Some(result) => {
            println!(
                "  ✓ Expired {} snapshots",
                result.stats.expired_snapshot_count
            );
        }
        None => println!("  ✓ Nothing to expire"),
    }

    // ── 8. Remove orphan files ─────────────────────────────────────────────────

    println!("\n── Step 4: Remove orphan files ──");
    let orphan_config = RemoveOrphanFilesConfig {
        // Only delete orphans older than 1 minute (use 7+ days in production).
        older_than: Duration::from_secs(60),
        dry_run: false,
        ..Default::default()
    };

    let orphan_remover =
        RemoveOrphanFilesBuilder::new(catalog.clone(), table_ident.clone(), orphan_config).build();

    let orphan_result = orphan_remover.execute().await?;
    println!(
        "  ✓ Found {} orphan files, deleted {}",
        orphan_result.stats.orphan_files_found, orphan_result.stats.orphan_files_deleted
    );
    if !orphan_result.orphan_file_paths.is_empty() {
        for path in &orphan_result.orphan_file_paths {
            println!("    - {path}");
        }
    }

    println!("\n✓ Table maintenance complete for {NAMESPACE}.{TABLE_NAME}");

    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Sets up the namespace — creates it if it does not already exist.
async fn setup_namespace(
    catalog: &dyn Catalog,
    ident: &NamespaceIdent,
) -> Result<(), Box<dyn std::error::Error>> {
    if catalog.namespace_exists(ident).await? {
        return Ok(());
    }

    // Location property is required by BigLake for the namespace.
    let mut props = HashMap::new();
    props.insert("location".to_owned(), GCS_BUCKET_LOCATION.to_owned());

    catalog.create_namespace(ident, props).await?;
    println!("  Created namespace '{}'", ident);
    Ok(())
}

/// Sets up the demo table fresh — drops it if it already exists, then creates it.
async fn setup_table(
    catalog: &dyn Catalog,
    namespace_ident: &NamespaceIdent,
    table_ident: &TableIdent,
) -> Result<Table, Box<dyn std::error::Error>> {
    if catalog.table_exists(table_ident).await? {
        catalog.drop_table(table_ident).await?;
        println!("  Dropped existing table '{TABLE_NAME}'");
    }

    let schema = demo_schema();
    let creation = TableCreation::builder()
        .name(TABLE_NAME.to_owned())
        .schema(schema)
        .build();

    let table = catalog.create_table(namespace_ident, creation).await?;
    println!("  Created table '{TABLE_NAME}'");
    Ok(table)
}

/// Returns the schema used by the demo table.
///
/// ```
/// id    LONG     — synthetic surrogate key
/// batch INT      — which append batch produced this row
/// value STRING   — padded text payload
/// ```
fn demo_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "batch", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "value", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("demo schema is valid")
}

/// Appends one batch of `ROWS_PER_BATCH` rows starting at `id_offset` and
/// commits the result as a new snapshot via a fast-append transaction.
///
/// Returns the updated table after the commit.
async fn append_batch(
    catalog: &dyn Catalog,
    table: &Table,
    id_offset: i32,
    run_id: &str,
    batch_index: usize,
) -> Result<Table, Box<dyn std::error::Error>> {
    let schema = table.metadata().current_schema();
    let file_io = table.file_io().clone();

    // Build the Arrow record batch
    let rows = ROWS_PER_BATCH as i64;
    let ids: Vec<i64> = (id_offset as i64..(id_offset as i64 + rows)).collect();
    let batches: Vec<i32> = vec![id_offset / ROWS_PER_BATCH as i32; ROWS_PER_BATCH];
    let values: Vec<String> = (0..ROWS_PER_BATCH)
        .map(|i| format!("row-{:06}", id_offset as usize + i))
        .collect();

    let arrow_schema = Arc::new(schema_to_arrow_schema(schema)?);
    let record_batch = RecordBatch::try_new(arrow_schema.clone(), vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(Int32Array::from(batches)),
        Arc::new(StringArray::from(values)),
    ])?;

    // Write the batch as a single Parquet data file using the correct writer chain:
    // ParquetWriterBuilder → RollingFileWriterBuilder → DataFileWriterBuilder
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
    // Include both the run ID and batch index so each call gets a unique file name
    // even though DefaultFileNameGenerator resets its counter per instance.
    let file_name_generator = DefaultFileNameGenerator::new(
        format!("batch-{run_id}-{batch_index:05}"),
        None,
        DataFileFormat::Parquet,
    );

    let parquet_writer_builder =
        ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());

    let rolling_writer_builder = RollingFileWriterBuilder::new(
        parquet_writer_builder,
        128 * 1024 * 1024, // 128 MB target file size
        file_io,
        location_generator,
        file_name_generator,
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

    let mut writer = data_file_writer_builder.build(None).await?;
    writer.write(record_batch).await?;
    let data_files = writer.close().await?;

    // Commit via fast-append transaction
    let txn = Transaction::new(table);
    let action = txn.fast_append().add_data_files(data_files);
    let txn = action.apply(txn)?;
    let updated_table = txn.commit(catalog).await?;

    Ok(updated_table)
}

/// Counts the total number of rows in the current snapshot.
///
/// First tries the snapshot's `added_rows_count()` (populated by operations
/// like compaction). Falls back to scanning manifest entries and summing
/// `record_count` for alive entries, using V1/V2-aware filtering.
async fn count_rows_via_manifest(table: &Table) -> Result<u64, Box<dyn std::error::Error>> {
    let metadata = table.metadata();
    let snapshot = metadata
        .current_snapshot()
        .ok_or("table has no current snapshot")?;

    // Prefer the snapshot-level row count when available (set by compaction).
    if let Some(count) = snapshot.added_rows_count() {
        return Ok(count);
    }

    // Fallback: scan manifest entries.
    let file_io = table.file_io();
    let manifest_list = snapshot
        .load_manifest_list(file_io, table.metadata())
        .await?;

    let is_v2 = metadata.format_version() >= iceberg::spec::FormatVersion::V2;
    let mut total_rows: u64 = 0;
    for manifest_file in manifest_list.entries() {
        let manifest_bytes = file_io
            .new_input(manifest_file.manifest_path.as_str())?
            .read()
            .await?;
        let manifest = Manifest::parse_avro(&manifest_bytes)?;
        for entry in manifest.entries() {
            let alive = if is_v2 {
                // V2: only count explicitly alive entries (not deleted).
                entry.is_alive()
            } else {
                // V1: all entries in a snapshot are implicitly alive.
                true
            };
            if alive {
                total_rows += entry.record_count();
            }
        }
    }
    Ok(total_rows)
}
