"""
Tests for catalog class instantiation and __repr__.

These are smoke tests only — no external services are contacted.
"""

from __future__ import annotations

import pytest

from iceberg_compaction import (
    MemoryCatalog,
    RestCatalog,
    GlueCatalog,
    HmsCatalog,
    S3TablesCatalog,
)


class TestMemoryCatalog:
    """MemoryCatalog instantiation and interface."""

    def test_repr(self, memory_catalog: MemoryCatalog) -> None:
        r = repr(memory_catalog)
        assert "MemoryCatalog" in r

    def test_instance_is_memory_catalog(self, memory_catalog: MemoryCatalog) -> None:
        # duck-typing check via protocol
        assert isinstance(memory_catalog, MemoryCatalog)


class TestRestCatalog:
    """RestCatalog instantiation — no network contact made."""

    def test_repr(self) -> None:
        # Uses a clearly-invalid URI so we can check __repr__ without hitting the network
        cat = RestCatalog("test_rest", uri="http://localhost:0/iceberg")
        r = repr(cat)
        assert "RestCatalog" in r

    def test_accepts_optional_warehouse(self) -> None:
        cat = RestCatalog(
            "test_rest",
            uri="http://localhost:0/iceberg",
            warehouse="s3://bucket/warehouse",
        )
        assert cat is not None

    def test_accepts_properties(self) -> None:
        cat = RestCatalog(
            "test_rest",
            uri="http://localhost:0/iceberg",
            properties={"token": "my-secret-token"},
        )
        assert cat is not None

    def test_accepts_google_auth(self) -> None:
        cat = RestCatalog(
            "test_rest",
            uri="http://localhost:0/iceberg",
            properties={"google-auth": "true"},
        )
        assert cat is not None


class TestGlueCatalog:
    """GlueCatalog instantiation."""

    def test_repr(self) -> None:
        # Uses a non-existent bucket — no AWS calls are made at construction
        cat = GlueCatalog("test_glue", warehouse="s3://nonexistent-bucket-xyz/")
        r = repr(cat)
        assert "GlueCatalog" in r

    def test_accepts_properties(self) -> None:
        cat = GlueCatalog(
            "test_glue",
            warehouse="s3://bucket/",
            properties={"region": "us-east-1"},
        )
        assert cat is not None


class TestHmsCatalog:
    """HmsCatalog instantiation.

    Note: HmsCatalog initialization involves connecting to the HMS thrift service.
    If no HMS server is available, the connection attempt may panic. These tests
    are marked ``skip`` unless an HMS endpoint is reachable.
    """

    @pytest.mark.skip(reason="requires a running HMS thrift server")
    def test_repr(self) -> None:
        cat = HmsCatalog(
            "test_hms",
            uri="localhost:9083",
            warehouse="s3://bucket/warehouse",
        )
        r = repr(cat)
        assert "HmsCatalog" in r

    @pytest.mark.skip(reason="requires a running HMS thrift server")
    def test_accepts_properties(self) -> None:
        cat = HmsCatalog(
            "test_hms",
            uri="localhost:9083",
            warehouse="s3://bucket/",
            properties={"thrift_transport": "framed"},
        )
        assert cat is not None


class TestS3TablesCatalog:
    """S3TablesCatalog instantiation."""

    def test_repr(self) -> None:
        cat = S3TablesCatalog(
            "test_s3tables",
            table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
        )
        r = repr(cat)
        assert "S3TablesCatalog" in r

    def test_accepts_properties(self) -> None:
        cat = S3TablesCatalog(
            "test_s3tables",
            table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
            properties={"region_name": "us-east-1"},
        )
        assert cat is not None
