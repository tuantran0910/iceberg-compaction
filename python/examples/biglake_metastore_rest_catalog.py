"""
BigLake Metastore end-to-end table maintenance example.

Mirrors examples/biglake-metastore-rest-catalog/src/main.rs — uses Google ADC
for authentication, pyiceberg for table creation and data appends, then
iceberg_compaction for all four maintenance operations.

Prerequisites:
- google-auth package (pip install google-auth)
- pyiceberg>=0.9.0 (already in dev dependencies)
- Valid Google ADC: gcloud auth application-default login
"""

from __future__ import annotations

from typing import Any

# BigLake configuration — must match the Rust example
GCP_PROJECT_ID = "rainbow-data-production-483609"
CATALOG_NAME = "rainbow-data-production-iceberg"
CATALOG_URI = "https://biglake.googleapis.com/iceberg/v1/restcatalog"
WAREHOUSE_PATH = "bq://projects/rainbow-data-production-483609"
GCS_BUCKET_LOCATION = "gs://rainbow-data-production-iceberg/test_iceberg_compactor"

NAMESPACE = "test_iceberg_compactor"
TABLE_NAME = "compaction_demo"
APPEND_BATCHES = 8
ROWS_PER_BATCH = 500


def setup_namespace(catalog: Any) -> None:
    """Create namespace if it does not already exist."""
    try:
        catalog.create_namespace(
            NAMESPACE, properties={"location": GCS_BUCKET_LOCATION}
        )
        print(f"  Created namespace '{NAMESPACE}'")
    except Exception:
        print(f"  Namespace '{NAMESPACE}' already exists")


def setup_table(catalog: Any) -> Any:
    """Drop the table if it exists and create it fresh."""
    from pyiceberg.schema import Schema, NestedField
    from pyiceberg.types import LongType, IntegerType, StringType

    full_name = f"{NAMESPACE}.{TABLE_NAME}"

    # Drop if exists
    try:
        catalog.drop_table(full_name)
        print(f"  Dropped existing table '{TABLE_NAME}'")
    except Exception:
        pass

    # Create fresh table
    schema = Schema(
        NestedField(1, "id", LongType()),
        NestedField(2, "batch", IntegerType()),
        NestedField(3, "value", StringType()),
    )
    table = catalog.create_table(full_name, schema=schema)
    print(f"  Created table '{TABLE_NAME}'")
    return table


def append_batches(table: Any) -> None:
    """Append multiple small data batches to build up snapshots."""
    import pyarrow as pa

    int32_type = pa.int32()

    for i in range(APPEND_BATCHES):
        ids = [i * ROWS_PER_BATCH + j for j in range(ROWS_PER_BATCH)]
        batches = pa.array([i] * ROWS_PER_BATCH, type=int32_type)
        values = [f"row-{i * ROWS_PER_BATCH + j:06}" for j in range(ROWS_PER_BATCH)]

        arrow_table = pa.table(
            {
                "id": ids,
                "batch": batches,
                "value": values,
            }
        )
        table.append(arrow_table)
        print(f"  Batch {i + 1}/{APPEND_BATCHES} committed")


def get_gcp_token() -> str:
    """Fetch ADC token using google-auth library."""
    import google.auth
    import google.auth.transport.requests

    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/devstorage.read_write"]
    )
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    return credentials.token


def main() -> None:
    from pyiceberg.catalog import load_catalog

    from iceberg_compaction import (
        Compaction,
        CompactionConfig,
        RemoveOrphanFiles,
        RemoveOrphanFilesConfig,
        ExpireSnapshots,
        ExpireSnapshotsConfig,
        RewriteManifests,
        RewriteManifestsConfig,
        RestCatalog,
    )

    # Fetch ADC token for both catalog auth and GCS FileIO
    bearer_token = get_gcp_token()
    print("  ✓ ADC token obtained via google-auth")

    # Build iceberg_compaction RestCatalog
    cat = RestCatalog(
        name=CATALOG_NAME,
        uri=CATALOG_URI,
        warehouse=WAREHOUSE_PATH,
        properties={
            "google-auth": "true",
            "gcs.project-id": GCP_PROJECT_ID,
            "gcs.user-project": GCP_PROJECT_ID,
            "gcs.oauth2.token": bearer_token,
            "header.x-goog-user-project": GCP_PROJECT_ID,
            "header.metrics.reporting-enabled": "false",
        },
    )
    print(f"  ✓ Connected to BigLake catalog '{CATALOG_NAME}'")

    pyiceberg_cat = load_catalog(
        CATALOG_NAME,
        type="rest",
        uri=CATALOG_URI,
        warehouse=WAREHOUSE_PATH,
        auth={"type": "google"},
        **{"header.x-goog-user-project": GCP_PROJECT_ID},
    )

    # Setup namespace and table
    print("Setting up namespace and table …")
    setup_namespace(pyiceberg_cat)
    table = setup_table(pyiceberg_cat)
    print(f"  ✓ Table ready: {NAMESPACE}.{TABLE_NAME}")

    # Append data batches
    print(f"\nAppending {APPEND_BATCHES} small data batches …")
    append_batches(table)
    print(f"  ✓ {APPEND_BATCHES} batches written")

    # ── Step 1: Compaction ─────────────────────────────────────────────────────

    print("\n── Step 1: Compaction ──")
    result = Compaction(
        cat,
        f"{NAMESPACE}.{TABLE_NAME}",
        CompactionConfig(
            strategy="small_files", target_file_size_bytes=128 * 1024 * 1024
        ),
    ).run()
    if result:
        print(
            f"  Compacted {result.input_files_count} → {result.output_files_count} files  "
            f"({result.input_bytes_total} → {result.output_bytes_total} bytes)"
        )
    else:
        print("  ✓ No compaction needed")

    # ── Step 2: Rewrite manifests ─────────────────────────────────────────────

    print("\n── Step 2: Rewrite manifests ──")
    result = RewriteManifests(
        cat,
        f"{NAMESPACE}.{TABLE_NAME}",
        RewriteManifestsConfig(),
    ).run()
    if result:
        print(
            f"  ✓ Rewrote manifests: {result.created_manifests_count} created, "
            f"{result.replaced_manifests_count} replaced "
            f"({result.processed_entry_count} entries processed)"
        )
    else:
        print("  ✓ Manifests already consolidated — no-op")

    # ── Step 3: Expire snapshots ───────────────────────────────────────────────

    print("\n── Step 3: Expire snapshots ──")
    result = ExpireSnapshots(
        cat,
        f"{NAMESPACE}.{TABLE_NAME}",
        ExpireSnapshotsConfig(retain_last=2, max_snapshot_age_secs=60),
    ).run()
    if result:
        print(f"  ✓ Expired {result.expired_snapshot_count} snapshots")
    else:
        print("  ✓ Nothing to expire")

    # ── Step 4: Remove orphan files ───────────────────────────────────────────

    print("\n── Step 4: Remove orphan files ──")
    result = RemoveOrphanFiles(
        cat,
        f"{NAMESPACE}.{TABLE_NAME}",
        RemoveOrphanFilesConfig(older_than_secs=60, dry_run=False),
    ).run()
    print(
        f"  ✓ Found {result.orphan_files_found} orphan files, "
        f"deleted {result.orphan_files_deleted}"
    )
    for path in result.orphan_file_paths:
        print(f"    - {path}")

    print(f"\n✓ Table maintenance complete for {NAMESPACE}.{TABLE_NAME}")


if __name__ == "__main__":
    main()
