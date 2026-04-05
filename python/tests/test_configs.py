"""
Tests for *Config classes — validation, defaults, and __repr__.
"""

from __future__ import annotations

import pytest

from iceberg_compaction import (
    CompactionConfig,
    RewriteManifestsConfig,
    ExpireSnapshotsConfig,
    RemoveOrphanFilesConfig,
    RetryConfig,
    ConfigError,
)


class TestCompactionConfig:
    """CompactionConfig validation and defaults."""

    def test_default_is_auto(self) -> None:
        cfg = CompactionConfig()
        assert cfg.strategy == "auto"
        assert cfg.target_file_size_bytes == 1_073_741_824
        assert cfg.small_file_threshold_bytes == 33_554_432
        assert cfg.grouping_strategy == "single"

    def test_all_strategiesAccepted(self) -> None:
        for strategy in ("auto", "small_files", "full", "files_with_deletes"):
            cfg = CompactionConfig(strategy=strategy)
            assert cfg.strategy == strategy

    def test_invalid_strategy_raises(self) -> None:
        with pytest.raises(ConfigError):
            CompactionConfig(strategy="invalid_strategy")

    def test_all_groupingStrategiesAccepted(self) -> None:
        for g in ("single", "bin_pack"):
            cfg = CompactionConfig(grouping_strategy=g)
            assert cfg.grouping_strategy == g

    def test_invalid_grouping_raises(self) -> None:
        with pytest.raises(ConfigError):
            CompactionConfig(grouping_strategy="unknown")

    def test_max_auto_plans_must_be_positive(self) -> None:
        with pytest.raises(ConfigError):
            CompactionConfig(max_auto_plans_per_run=0)

    def test_max_auto_plans_none_is_fine(self) -> None:
        cfg = CompactionConfig(max_auto_plans_per_run=None)
        assert cfg.max_auto_plans_per_run is None

    def test_repr(self) -> None:
        cfg = CompactionConfig(strategy="small_files")
        r = repr(cfg)
        assert "CompactionConfig" in r
        assert "small_files" in r


class TestRewriteManifestsConfig:
    """RewriteManifestsConfig validation and defaults."""

    def test_default_single_clustering(self) -> None:
        cfg = RewriteManifestsConfig()
        assert cfg.clustering_strategy == "single"
        assert cfg.snapshot_properties == {}

    def test_valid_clusteringStrategies(self) -> None:
        for s in ("single", "by_partition_spec_id"):
            cfg = RewriteManifestsConfig(clustering_strategy=s)
            assert cfg.clustering_strategy == s

    def test_invalid_clustering_raises(self) -> None:
        with pytest.raises(ConfigError):
            RewriteManifestsConfig(clustering_strategy="invalid")

    def test_snapshot_properties(self) -> None:
        props = {"key": "value", "foo": "bar"}
        cfg = RewriteManifestsConfig(snapshot_properties=props)
        assert cfg.snapshot_properties == props

    def test_repr(self) -> None:
        cfg = RewriteManifestsConfig(clustering_strategy="by_partition_spec_id")
        r = repr(cfg)
        assert "RewriteManifestsConfig" in r
        assert "by_partition_spec_id" in r


class TestExpireSnapshotsConfig:
    """ExpireSnapshotsConfig defaults and validation."""

    def test_defaults(self) -> None:
        cfg = ExpireSnapshotsConfig()
        assert cfg.retain_last == 1
        assert cfg.max_snapshot_age_secs == 432_000
        assert cfg.snapshot_ids_to_expire == []
        assert cfg.clear_expired_metadata is False

    def test_custom_values(self) -> None:
        ids = [1, 2, 3]
        cfg = ExpireSnapshotsConfig(
            retain_last=5,
            max_snapshot_age_secs=86400,
            snapshot_ids_to_expire=ids,
            clear_expired_metadata=True,
        )
        assert cfg.retain_last == 5
        assert cfg.max_snapshot_age_secs == 86400
        assert cfg.snapshot_ids_to_expire == ids
        assert cfg.clear_expired_metadata is True

    def test_repr(self) -> None:
        cfg = ExpireSnapshotsConfig(retain_last=10)
        r = repr(cfg)
        assert "ExpireSnapshotsConfig" in r
        assert "10" in r


class TestRemoveOrphanFilesConfig:
    """RemoveOrphanFilesConfig defaults."""

    def test_defaults(self) -> None:
        cfg = RemoveOrphanFilesConfig()
        assert cfg.older_than_secs == 604_800
        assert cfg.dry_run is False
        assert cfg.load_concurrency == 16
        assert cfg.delete_concurrency == 10

    def test_dry_run_true(self) -> None:
        cfg = RemoveOrphanFilesConfig(dry_run=True)
        assert cfg.dry_run is True

    def test_repr(self) -> None:
        cfg = RemoveOrphanFilesConfig(older_than_secs=3600, dry_run=True)
        r = repr(cfg)
        assert "RemoveOrphanFilesConfig" in r
        assert "dry_run=True" in r


class TestRetryConfig:
    """RetryConfig defaults."""

    def test_defaults(self) -> None:
        cfg = RetryConfig()
        assert cfg.max_retries == 3
        assert cfg.initial_delay_secs == 1
        assert cfg.max_delay_secs == 10

    def test_custom_values(self) -> None:
        cfg = RetryConfig(max_retries=5, initial_delay_secs=2, max_delay_secs=60)
        assert cfg.max_retries == 5
        assert cfg.initial_delay_secs == 2
        assert cfg.max_delay_secs == 60

    def test_repr(self) -> None:
        cfg = RetryConfig(max_retries=10)
        r = repr(cfg)
        assert "RetryConfig" in r
        assert "10" in r
