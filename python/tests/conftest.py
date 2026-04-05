"""
Shared pytest fixtures for iceberg-compaction Python tests.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from iceberg_compaction import (
    MemoryCatalog,
)


@pytest.fixture
def temp_warehouse(tmp_path: Path) -> str:
    """Temporary filesystem path for MemoryCatalog warehouse."""
    return str(tmp_path / "warehouse")


@pytest.fixture
def memory_catalog(temp_warehouse: str) -> MemoryCatalog:
    """A MemoryCatalog backed by a temporary directory."""
    return MemoryCatalog("test_catalog", temp_warehouse)
