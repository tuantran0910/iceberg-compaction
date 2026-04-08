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

use std::borrow::Cow;
use std::sync::Arc;

use mixtrics::metrics::{BoxedCounterVec, BoxedHistogramVec, BoxedRegistry, Buckets};

use crate::executor::RewriteFilesStat;
use crate::expire_snapshots::ExpireSnapshotsStats;
use crate::manifest_rewrite::RewriteManifestsStats;
use crate::remove_orphan_files::RemoveOrphanFilesStats;

pub struct Metrics {
    // commit metrics
    pub compaction_commit_counter: BoxedCounterVec,
    pub compaction_duration: BoxedHistogramVec,
    pub compaction_commit_duration: BoxedHistogramVec,
    pub compaction_commit_failed_counter: BoxedCounterVec,
    pub compaction_executor_error_counter: BoxedCounterVec,

    // New hierarchical metrics for plan-level analysis
    pub compaction_plan_execution_duration: BoxedHistogramVec, // Individual plan execution time
    pub compaction_plan_file_count: BoxedHistogramVec,         // Number of files processed per plan
    pub compaction_plan_size_bytes: BoxedHistogramVec,         // Bytes processed per plan

    // input/output metrics
    pub compaction_input_files_count: BoxedCounterVec,
    pub compaction_output_files_count: BoxedCounterVec,
    pub compaction_input_bytes_total: BoxedCounterVec,
    pub compaction_output_bytes_total: BoxedCounterVec,

    // DataFusion processing metrics
    pub compaction_datafusion_records_processed_total: BoxedCounterVec,
    pub compaction_datafusion_batch_fetch_duration: BoxedHistogramVec,
    pub compaction_datafusion_batch_write_duration: BoxedHistogramVec,
    pub compaction_datafusion_bytes_processed_total: BoxedCounterVec,

    // DataFusion distribution metrics
    pub compaction_datafusion_batch_row_count_dist: BoxedHistogramVec,
    pub compaction_datafusion_batch_bytes_dist: BoxedHistogramVec,

    // Manifest rewrite metrics
    pub compaction_manifest_rewrite_triggered_counter: BoxedCounterVec,
    pub compaction_manifest_rewrite_success_counter: BoxedCounterVec,
    pub compaction_manifest_rewrite_noop_counter: BoxedCounterVec,
    pub compaction_manifest_rewrite_failed_counter: BoxedCounterVec,
    pub compaction_manifest_rewrite_duration: BoxedHistogramVec,
    pub compaction_manifest_rewrite_created_total: BoxedCounterVec,
    pub compaction_manifest_rewrite_replaced_total: BoxedCounterVec,
    pub compaction_manifest_rewrite_entries_processed_total: BoxedCounterVec,

    // Expire snapshots metrics
    pub expire_snapshots_triggered_counter: BoxedCounterVec,
    pub expire_snapshots_success_counter: BoxedCounterVec,
    pub expire_snapshots_noop_counter: BoxedCounterVec,
    pub expire_snapshots_failed_counter: BoxedCounterVec,
    pub expire_snapshots_duration: BoxedHistogramVec,
    pub expire_snapshots_expired_snapshots_total: BoxedCounterVec,

    // Remove orphan files metrics
    pub remove_orphan_files_triggered_counter: BoxedCounterVec,
    pub remove_orphan_files_success_counter: BoxedCounterVec,
    pub remove_orphan_files_failed_counter: BoxedCounterVec,
    pub remove_orphan_files_duration: BoxedHistogramVec,
    pub remove_orphan_files_found_total: BoxedCounterVec,
    pub remove_orphan_files_deleted_total: BoxedCounterVec,
}

impl Metrics {
    pub fn new(registry: BoxedRegistry) -> Self {
        // Bucket constants for large-scale compaction support
        // Designed to handle: ~1 hour duration, ~1TB data size, ~4096 files
        const COMPACTION_DURATION_BUCKET_START_MS: f64 = 1000.0; // 1s
        const COMPACTION_DURATION_BUCKET_FACTOR: f64 = 4.0; // x4 per bucket
        const COMPACTION_DURATION_BUCKET_COUNT: usize = 8; // 8 buckets: 1s ~ 16384s (~4.5 hours)

        const PLAN_EXEC_DURATION_BUCKET_START_MS: f64 = 1000.0; // 1s
        const PLAN_EXEC_DURATION_BUCKET_FACTOR: f64 = 4.0; // x4 per bucket
        const PLAN_EXEC_DURATION_BUCKET_COUNT: usize = 8; // 8 buckets: 1s ~ 16384s (~4.5 hours)

        const PLAN_FILE_COUNT_BUCKET_START: f64 = 1.0; // 1 file
        const PLAN_FILE_COUNT_BUCKET_FACTOR: f64 = 2.0; // x2 per bucket
        const PLAN_FILE_COUNT_BUCKET_COUNT: usize = 13; // 13 buckets: 1 ~ 4096 files

        const PLAN_SIZE_BUCKET_START_BYTES: f64 = 1024.0 * 1024.0; // 1MB
        const PLAN_SIZE_BUCKET_FACTOR: f64 = 4.0; // x4 per bucket
        const PLAN_SIZE_BUCKET_COUNT: usize = 12; // 12 buckets: 1MB ~ 16TB

        let compaction_commit_counter = registry.register_counter_vec(
            "iceberg_compaction_commit_counter".into(),
            "iceberg-compaction compaction total commit counts".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_duration = registry.register_histogram_vec_with_buckets(
            "iceberg_compaction_duration".into(),
            "iceberg-compaction compaction duration in milliseconds".into(),
            &["catalog_name", "table_ident"],
            Buckets::exponential(
                COMPACTION_DURATION_BUCKET_START_MS,
                COMPACTION_DURATION_BUCKET_FACTOR,
                COMPACTION_DURATION_BUCKET_COUNT,
            ),
        );

        // 10ms 100ms 1s 10s 100s
        let compaction_commit_duration = registry.register_histogram_vec_with_buckets(
            "iceberg_compaction_commit_duration".into(),
            "iceberg-compaction compaction commit duration in milliseconds".into(),
            &["catalog_name", "table_ident"],
            Buckets::exponential(
                10.0, 10.0, 5, // Start at 10ms, multiply each bucket by 10, up to 5 buckets
            ),
        );

        let compaction_commit_failed_counter = registry.register_counter_vec(
            "iceberg_compaction_commit_failed_counter".into(),
            "iceberg-compaction compaction commit failed counts".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_executor_error_counter = registry.register_counter_vec(
            "iceberg_compaction_executor_error_counter".into(),
            "iceberg-compaction compaction executor error counts".into(),
            &["catalog_name", "table_ident"],
        );

        // === New plan-level metrics ===
        let compaction_plan_execution_duration = registry.register_histogram_vec_with_buckets(
            "iceberg_compaction_plan_execution_duration".into(),
            "Duration for executing individual compaction plans in milliseconds".into(),
            &["catalog_name", "table_ident"],
            Buckets::exponential(
                PLAN_EXEC_DURATION_BUCKET_START_MS,
                PLAN_EXEC_DURATION_BUCKET_FACTOR,
                PLAN_EXEC_DURATION_BUCKET_COUNT,
            ),
        );

        let compaction_plan_file_count = registry.register_histogram_vec_with_buckets(
            "iceberg_compaction_plan_file_count".into(),
            "Number of files processed by individual compaction plans".into(),
            &["catalog_name", "table_ident"],
            Buckets::exponential(
                PLAN_FILE_COUNT_BUCKET_START,
                PLAN_FILE_COUNT_BUCKET_FACTOR,
                PLAN_FILE_COUNT_BUCKET_COUNT,
            ),
        );

        let compaction_plan_size_bytes = registry.register_histogram_vec_with_buckets(
            "iceberg_compaction_plan_size_bytes".into(),
            "Bytes processed by individual compaction plans".into(),
            &["catalog_name", "table_ident"],
            Buckets::exponential(
                PLAN_SIZE_BUCKET_START_BYTES,
                PLAN_SIZE_BUCKET_FACTOR,
                PLAN_SIZE_BUCKET_COUNT,
            ),
        );

        // === Input/Output metrics registration ===
        let compaction_input_files_count = registry.register_counter_vec(
            "iceberg_compaction_input_files_count".into(),
            "Number of input files being compacted".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_output_files_count = registry.register_counter_vec(
            "iceberg_compaction_output_files_count".into(),
            "Number of output files from compaction".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_input_bytes_total = registry.register_counter_vec(
            "iceberg_compaction_input_bytes_total".into(),
            "Total number of bytes in input files for compaction".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_output_bytes_total = registry.register_counter_vec(
            "iceberg_compaction_output_bytes_total".into(),
            "Total number of bytes in output files from compaction".into(),
            &["catalog_name", "table_ident"],
        );

        // === DataFusion processing metrics ===
        let compaction_datafusion_records_processed_total = registry.register_counter_vec(
            "iceberg_compaction_datafusion_records_processed_total".into(),
            "Total number of records processed by DataFusion during compaction".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_datafusion_batch_fetch_duration = registry
            .register_histogram_vec_with_buckets(
                "iceberg_compaction_datafusion_batch_fetch_duration".into(),
                "Duration of fetching individual record batches in DataFusion (milliseconds)"
                    .into(),
                &["catalog_name", "table_ident"],
                Buckets::exponential(
                    1.0, 10.0, 6, // 1ms, 10ms, 100ms, 1s, 10s, 100s
                ),
            );

        let compaction_datafusion_batch_write_duration = registry
            .register_histogram_vec_with_buckets(
                "iceberg_compaction_datafusion_batch_write_duration".into(),
                "Duration of writing individual record batches in DataFusion (milliseconds)".into(),
                &["catalog_name", "table_ident"],
                Buckets::exponential(
                    1.0, 10.0, 6, // 1ms, 10ms, 100ms, 1s, 10s, 100s
                ),
            );

        let compaction_datafusion_bytes_processed_total = registry.register_counter_vec(
            "iceberg_compaction_datafusion_bytes_processed_total".into(),
            "Total number of bytes processed by DataFusion during compaction".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_datafusion_batch_row_count_dist = registry
            .register_histogram_vec_with_buckets(
                "iceberg_compaction_datafusion_batch_row_count_dist".into(),
                "Distribution of row counts in record batches processed by DataFusion".into(),
                &["catalog_name", "table_ident"],
                Buckets::exponential(100.0, 2.0, 10), // 100, 200, 400, ..., 51200 rows
            );

        let compaction_datafusion_batch_bytes_dist = registry.register_histogram_vec_with_buckets(
            "iceberg_compaction_datafusion_batch_bytes_dist".into(),
            "Distribution of byte sizes of record batches processed by DataFusion".into(),
            &["catalog_name", "table_ident"],
            Buckets::exponential(1024.0 * 64.0, 2.0, 12), // 64KB, 128KB, 256KB, ..., 128MB
        );

        // === Manifest rewrite metrics ===
        let compaction_manifest_rewrite_triggered_counter = registry.register_counter_vec(
            "iceberg_compaction_manifest_rewrite_triggered_total".into(),
            "Number of times manifest rewrite was triggered (threshold exceeded)".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_manifest_rewrite_success_counter = registry.register_counter_vec(
            "iceberg_compaction_manifest_rewrite_success_total".into(),
            "Number of times manifest rewrite produced a new snapshot".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_manifest_rewrite_noop_counter = registry.register_counter_vec(
            "iceberg_compaction_manifest_rewrite_noop_total".into(),
            "Number of times manifest rewrite was a no-op (≤1 manifest)".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_manifest_rewrite_failed_counter = registry.register_counter_vec(
            "iceberg_compaction_manifest_rewrite_failed_total".into(),
            "Number of times manifest rewrite failed".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_manifest_rewrite_duration = registry.register_histogram_vec_with_buckets(
            "iceberg_compaction_manifest_rewrite_duration_ms".into(),
            "Duration of manifest rewrite operations in milliseconds".into(),
            &["catalog_name", "table_ident"],
            Buckets::exponential(1000.0, 4.0, 8), // 1s to ~4.5h
        );

        let compaction_manifest_rewrite_created_total = registry.register_counter_vec(
            "iceberg_compaction_manifest_rewrite_created_total".into(),
            "New manifest files written during manifest rewriting".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_manifest_rewrite_replaced_total = registry.register_counter_vec(
            "iceberg_compaction_manifest_rewrite_replaced_total".into(),
            "Old manifest files replaced during manifest rewriting".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_manifest_rewrite_entries_processed_total = registry.register_counter_vec(
            "iceberg_compaction_manifest_rewrite_entries_processed_total".into(),
            "Data-file entries processed during manifest rewriting".into(),
            &["catalog_name", "table_ident"],
        );

        // === Expire snapshots metrics ===
        let expire_snapshots_triggered_counter = registry.register_counter_vec(
            "iceberg_expire_snapshots_triggered_total".into(),
            "Number of times expire snapshots was invoked".into(),
            &["catalog_name", "table_ident"],
        );

        let expire_snapshots_success_counter = registry.register_counter_vec(
            "iceberg_expire_snapshots_success_total".into(),
            "Number of times expire snapshots produced a metadata commit".into(),
            &["catalog_name", "table_ident"],
        );

        let expire_snapshots_noop_counter = registry.register_counter_vec(
            "iceberg_expire_snapshots_noop_total".into(),
            "Number of times expire snapshots was a no-op (nothing to expire)".into(),
            &["catalog_name", "table_ident"],
        );

        let expire_snapshots_failed_counter = registry.register_counter_vec(
            "iceberg_expire_snapshots_failed_total".into(),
            "Number of times expire snapshots failed".into(),
            &["catalog_name", "table_ident"],
        );

        let expire_snapshots_duration = registry.register_histogram_vec_with_buckets(
            "iceberg_expire_snapshots_duration_ms".into(),
            "Duration of expire snapshots operations in milliseconds".into(),
            &["catalog_name", "table_ident"],
            Buckets::exponential(1000.0, 4.0, 8), // 1s to ~4.5h
        );

        let expire_snapshots_expired_snapshots_total = registry.register_counter_vec(
            "iceberg_expire_snapshots_expired_snapshots_total".into(),
            "Total number of snapshots expired across all operations".into(),
            &["catalog_name", "table_ident"],
        );

        // === Remove orphan files metrics ===
        let remove_orphan_files_triggered_counter = registry.register_counter_vec(
            "iceberg_remove_orphan_files_triggered_total".into(),
            "Number of times remove orphan files was invoked".into(),
            &["catalog_name", "table_ident"],
        );

        let remove_orphan_files_success_counter = registry.register_counter_vec(
            "iceberg_remove_orphan_files_success_total".into(),
            "Number of times remove orphan files completed successfully".into(),
            &["catalog_name", "table_ident"],
        );

        let remove_orphan_files_failed_counter = registry.register_counter_vec(
            "iceberg_remove_orphan_files_failed_total".into(),
            "Number of times remove orphan files failed".into(),
            &["catalog_name", "table_ident"],
        );

        let remove_orphan_files_duration = registry.register_histogram_vec_with_buckets(
            "iceberg_remove_orphan_files_duration_ms".into(),
            "Duration of remove orphan files operations in milliseconds".into(),
            &["catalog_name", "table_ident"],
            Buckets::exponential(1000.0, 4.0, 8), // 1s to ~4.5h
        );

        let remove_orphan_files_found_total = registry.register_counter_vec(
            "iceberg_remove_orphan_files_found_total".into(),
            "Total number of orphan files identified across all operations".into(),
            &["catalog_name", "table_ident"],
        );

        let remove_orphan_files_deleted_total = registry.register_counter_vec(
            "iceberg_remove_orphan_files_deleted_total".into(),
            "Total number of orphan files deleted across all operations".into(),
            &["catalog_name", "table_ident"],
        );

        Self {
            compaction_commit_counter,
            compaction_duration,
            compaction_commit_duration,
            compaction_commit_failed_counter,
            compaction_executor_error_counter,

            // New plan-level metrics
            compaction_plan_execution_duration,
            compaction_plan_file_count,
            compaction_plan_size_bytes,

            compaction_input_files_count,
            compaction_output_files_count,
            compaction_input_bytes_total,
            compaction_output_bytes_total,

            // datafusion metrics
            compaction_datafusion_records_processed_total,
            compaction_datafusion_batch_fetch_duration,
            compaction_datafusion_batch_write_duration,
            compaction_datafusion_bytes_processed_total,
            compaction_datafusion_batch_row_count_dist,
            compaction_datafusion_batch_bytes_dist,

            // manifest rewrite metrics
            compaction_manifest_rewrite_triggered_counter,
            compaction_manifest_rewrite_success_counter,
            compaction_manifest_rewrite_noop_counter,
            compaction_manifest_rewrite_failed_counter,
            compaction_manifest_rewrite_duration,
            compaction_manifest_rewrite_created_total,
            compaction_manifest_rewrite_replaced_total,
            compaction_manifest_rewrite_entries_processed_total,

            // expire snapshots metrics
            expire_snapshots_triggered_counter,
            expire_snapshots_success_counter,
            expire_snapshots_noop_counter,
            expire_snapshots_failed_counter,
            expire_snapshots_duration,
            expire_snapshots_expired_snapshots_total,

            // remove orphan files metrics
            remove_orphan_files_triggered_counter,
            remove_orphan_files_success_counter,
            remove_orphan_files_failed_counter,
            remove_orphan_files_duration,
            remove_orphan_files_found_total,
            remove_orphan_files_deleted_total,
        }
    }
}

/// Helper for recording compaction metrics
/// Focuses on business-level metrics that can be accurately measured
#[derive(Clone)]
pub struct CompactionMetricsRecorder {
    metrics: Arc<Metrics>,
    catalog_name: Cow<'static, str>,
    table_ident: Cow<'static, str>,
}

impl CompactionMetricsRecorder {
    pub fn new(
        metrics: Arc<Metrics>,
        catalog_name: impl Into<Cow<'static, str>>,
        table_ident: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self {
            metrics,
            catalog_name: catalog_name.into(),
            table_ident: table_ident.into(),
        }
    }

    /// Helper to create label vector for metrics
    fn label_vec(&self) -> [std::borrow::Cow<'static, str>; 2] {
        [self.catalog_name.clone(), self.table_ident.clone()]
    }

    /// Record compaction duration (milliseconds)
    pub fn record_compaction_duration(&self, duration_ms: f64) {
        if duration_ms == 0.0 || !duration_ms.is_finite() {
            return; // Avoid recording zero duration
        }

        let label_vec = self.label_vec();
        self.metrics
            .compaction_duration
            .histogram(&label_vec)
            .record(duration_ms);
    }

    /// Record commit duration (milliseconds)
    pub fn record_commit_duration(&self, duration_ms: f64) {
        if duration_ms == 0.0 || !duration_ms.is_finite() {
            return; // Avoid recording zero duration
        }

        let label_vec = self.label_vec();
        self.metrics
            .compaction_commit_duration
            .histogram(&label_vec)
            .record(duration_ms);
    }

    /// Record individual plan execution duration (milliseconds)
    pub fn record_plan_execution_duration(&self, duration_ms: f64) {
        if duration_ms == 0.0 || !duration_ms.is_finite() {
            return; // Avoid recording zero duration
        }

        let label_vec = self.label_vec();
        self.metrics
            .compaction_plan_execution_duration
            .histogram(&label_vec)
            .record(duration_ms);
    }

    /// Record the number of files processed by a plan
    pub fn record_plan_file_count(&self, file_count: usize) {
        if file_count == 0 {
            return; // Avoid recording zero file count
        }

        let label_vec = self.label_vec();

        self.metrics
            .compaction_plan_file_count
            .histogram(&label_vec)
            .record(file_count as f64);
    }

    /// Record the bytes processed by a plan
    pub fn record_plan_size_bytes(&self, size_bytes: u64) {
        if size_bytes == 0 {
            return; // Avoid recording zero size
        }

        let label_vec = self.label_vec();

        self.metrics
            .compaction_plan_size_bytes
            .histogram(&label_vec)
            .record(size_bytes as f64);
    }

    /// Record successful compaction commit
    pub fn record_commit_success(&self) {
        let label_vec = self.label_vec();

        self.metrics
            .compaction_commit_counter
            .counter(&label_vec)
            .increase(1);
    }

    /// Record compaction commit failure
    pub fn record_commit_failure(&self) {
        let label_vec = self.label_vec();

        self.metrics
            .compaction_commit_failed_counter
            .counter(&label_vec)
            .increase(1);
    }

    /// Record executor error
    pub fn record_executor_error(&self) {
        let label_vec = self.label_vec();

        self.metrics
            .compaction_executor_error_counter
            .counter(&label_vec)
            .increase(1);
    }

    /// Record complete compaction metrics
    /// This is a convenience method that records all basic compaction metrics
    pub fn record_compaction_complete(&self, stats: &RewriteFilesStat) {
        if stats.input_files_count == 0 && stats.output_files_count == 0 {
            return; // No files processed, skip metrics
        }

        let label_vec = self.label_vec();

        if stats.input_files_count > 0 {
            self.metrics
                .compaction_input_files_count
                .counter(&label_vec)
                .increase(stats.input_files_count as u64);
        }

        if stats.input_total_bytes > 0 {
            self.metrics
                .compaction_input_bytes_total
                .counter(&label_vec)
                .increase(stats.input_total_bytes);
        }

        // output
        if stats.output_files_count > 0 {
            self.metrics
                .compaction_output_files_count
                .counter(&label_vec)
                .increase(stats.output_files_count as u64);
        }

        if stats.output_total_bytes > 0 {
            self.metrics
                .compaction_output_bytes_total
                .counter(&label_vec)
                .increase(stats.output_total_bytes);
        }
    }

    pub fn record_datafusion_batch_fetch_duration(&self, fetch_duration_ms: f64) {
        if fetch_duration_ms <= 0.0 || !fetch_duration_ms.is_finite() {
            return; // Avoid recording zero, negative, or invalid durations
        }

        let label_vec = self.label_vec();

        self.metrics
            .compaction_datafusion_batch_fetch_duration
            .histogram(&label_vec)
            .record(fetch_duration_ms); // Already in milliseconds
    }

    pub fn record_datafusion_batch_write_duration(&self, write_duration_ms: f64) {
        if write_duration_ms <= 0.0 || !write_duration_ms.is_finite() {
            return; // Avoid recording zero, negative, or invalid durations
        }

        let label_vec = self.label_vec();

        self.metrics
            .compaction_datafusion_batch_write_duration
            .histogram(&label_vec)
            .record(write_duration_ms); // Already in milliseconds
    }

    pub fn record_batch_stats(&self, record_count: u64, batch_bytes: u64) {
        if record_count == 0 && batch_bytes == 0 {
            return; // No records or bytes, skip metrics
        }

        let label_vec = self.label_vec();

        if record_count > 0 {
            self.metrics
                .compaction_datafusion_records_processed_total
                .counter(&label_vec)
                .increase(record_count);

            self.metrics
                .compaction_datafusion_batch_row_count_dist
                .histogram(&label_vec)
                .record(record_count as f64);
        }

        if batch_bytes > 0 {
            self.metrics
                .compaction_datafusion_bytes_processed_total
                .counter(&label_vec)
                .increase(batch_bytes);

            self.metrics
                .compaction_datafusion_batch_bytes_dist
                .histogram(&label_vec)
                .record(batch_bytes as f64);
        }
    }

    /// Record manifest rewrite triggered (threshold check passed)
    pub fn record_manifest_rewrite_triggered(&self) {
        let label_vec = self.label_vec();
        self.metrics
            .compaction_manifest_rewrite_triggered_counter
            .counter(&label_vec)
            .increase(1);
    }

    /// Record manifest rewrite success (produced a new snapshot)
    pub fn record_manifest_rewrite_success(&self, stats: &RewriteManifestsStats) {
        let label_vec = self.label_vec();
        self.metrics
            .compaction_manifest_rewrite_success_counter
            .counter(&label_vec)
            .increase(1);
        if stats.created_manifests_count > 0 {
            self.metrics
                .compaction_manifest_rewrite_created_total
                .counter(&label_vec)
                .increase(stats.created_manifests_count);
        }
        if stats.replaced_manifests_count > 0 {
            self.metrics
                .compaction_manifest_rewrite_replaced_total
                .counter(&label_vec)
                .increase(stats.replaced_manifests_count);
        }
        if stats.processed_entry_count > 0 {
            self.metrics
                .compaction_manifest_rewrite_entries_processed_total
                .counter(&label_vec)
                .increase(stats.processed_entry_count);
        }
    }

    /// Record manifest rewrite was a no-op
    pub fn record_manifest_rewrite_noop(&self) {
        let label_vec = self.label_vec();
        self.metrics
            .compaction_manifest_rewrite_noop_counter
            .counter(&label_vec)
            .increase(1);
    }

    /// Record manifest rewrite failure
    pub fn record_manifest_rewrite_failed(&self) {
        let label_vec = self.label_vec();
        self.metrics
            .compaction_manifest_rewrite_failed_counter
            .counter(&label_vec)
            .increase(1);
    }

    /// Record manifest rewrite duration
    pub fn record_manifest_rewrite_duration(&self, duration_ms: f64) {
        if duration_ms == 0.0 || !duration_ms.is_finite() {
            return;
        }
        let label_vec = self.label_vec();
        self.metrics
            .compaction_manifest_rewrite_duration
            .histogram(&label_vec)
            .record(duration_ms);
    }

    /// Record that expire snapshots was triggered.
    pub fn record_expire_snapshots_triggered(&self) {
        let label_vec = self.label_vec();
        self.metrics
            .expire_snapshots_triggered_counter
            .counter(&label_vec)
            .increase(1);
    }

    /// Record a successful expire snapshots operation that produced a commit.
    pub fn record_expire_snapshots_success(&self, stats: &ExpireSnapshotsStats, duration_ms: f64) {
        let label_vec = self.label_vec();
        self.metrics
            .expire_snapshots_success_counter
            .counter(&label_vec)
            .increase(1);
        if stats.expired_snapshot_count > 0 {
            self.metrics
                .expire_snapshots_expired_snapshots_total
                .counter(&label_vec)
                .increase(stats.expired_snapshot_count);
        }
        if duration_ms > 0.0 && duration_ms.is_finite() {
            self.metrics
                .expire_snapshots_duration
                .histogram(&label_vec)
                .record(duration_ms);
        }
    }

    /// Record an expire snapshots no-op (nothing was expired).
    pub fn record_expire_snapshots_noop(&self, duration_ms: f64) {
        let label_vec = self.label_vec();
        self.metrics
            .expire_snapshots_noop_counter
            .counter(&label_vec)
            .increase(1);
        if duration_ms > 0.0 && duration_ms.is_finite() {
            self.metrics
                .expire_snapshots_duration
                .histogram(&label_vec)
                .record(duration_ms);
        }
    }

    /// Record an expire snapshots failure.
    pub fn record_expire_snapshots_failed(&self) {
        let label_vec = self.label_vec();
        self.metrics
            .expire_snapshots_failed_counter
            .counter(&label_vec)
            .increase(1);
    }

    /// Record that remove orphan files was triggered.
    pub fn record_remove_orphan_files_triggered(&self) {
        let label_vec = self.label_vec();
        self.metrics
            .remove_orphan_files_triggered_counter
            .counter(&label_vec)
            .increase(1);
    }

    /// Record a successful remove orphan files operation.
    pub fn record_remove_orphan_files_success(
        &self,
        stats: &RemoveOrphanFilesStats,
        duration_ms: f64,
    ) {
        let label_vec = self.label_vec();
        self.metrics
            .remove_orphan_files_success_counter
            .counter(&label_vec)
            .increase(1);
        if stats.orphan_files_found > 0 {
            self.metrics
                .remove_orphan_files_found_total
                .counter(&label_vec)
                .increase(stats.orphan_files_found);
        }
        if stats.orphan_files_deleted > 0 {
            self.metrics
                .remove_orphan_files_deleted_total
                .counter(&label_vec)
                .increase(stats.orphan_files_deleted);
        }
        if duration_ms > 0.0 && duration_ms.is_finite() {
            self.metrics
                .remove_orphan_files_duration
                .histogram(&label_vec)
                .record(duration_ms);
        }
    }

    /// Record a remove orphan files failure.
    pub fn record_remove_orphan_files_failed(&self) {
        let label_vec = self.label_vec();
        self.metrics
            .remove_orphan_files_failed_counter
            .counter(&label_vec)
            .increase(1);
    }
}
