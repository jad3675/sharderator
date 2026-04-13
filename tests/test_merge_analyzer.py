"""Tests for merge analyzer — grouping logic and mapping union."""

from sharderator.engine.merge_analyzer import propose_merges, union_mappings
from sharderator.models.index_info import IndexInfo


def _make_index(name: str, shards: int = 1, docs: int = 100, size: int = 1000) -> IndexInfo:
    return IndexInfo(name=name, shard_count=shards, doc_count=docs, store_size_bytes=size)


class TestProposeMerges:
    def test_groups_by_monthly_bucket(self):
        indices = [
            _make_index("partial-.ds-metrics-cpu-default-2026.02.01-000001"),
            _make_index("partial-.ds-metrics-cpu-default-2026.02.15-000002"),
            _make_index("partial-.ds-metrics-cpu-default-2026.03.01-000003"),
            _make_index("partial-.ds-metrics-cpu-default-2026.03.15-000004"),
        ]
        groups = propose_merges(indices, "monthly")
        assert len(groups) == 2
        assert groups[0].time_bucket in ("2026.02", "2026.03")
        assert all(len(g.source_indices) == 2 for g in groups)

    def test_quarterly_grouping(self):
        indices = [
            _make_index("partial-.ds-logs-app-2026.01.01-000001"),
            _make_index("partial-.ds-logs-app-2026.02.01-000002"),
            _make_index("partial-.ds-logs-app-2026.03.01-000003"),
        ]
        groups = propose_merges(indices, "quarterly")
        assert len(groups) == 1
        assert groups[0].time_bucket == "2026.Q1"
        assert len(groups[0].source_indices) == 3

    def test_single_index_excluded(self):
        indices = [_make_index("partial-.ds-metrics-cpu-default-2026.02.01-000001")]
        groups = propose_merges(indices, "monthly")
        assert len(groups) == 0

    def test_non_matching_names_skipped(self):
        indices = [
            _make_index("some-random-index"),
            _make_index("another-one"),
        ]
        groups = propose_merges(indices, "monthly")
        assert len(groups) == 0

    def test_shard_reduction_calculation(self):
        indices = [
            _make_index("partial-.ds-m-2026.02.01-001", shards=1),
            _make_index("partial-.ds-m-2026.02.02-002", shards=1),
            _make_index("partial-.ds-m-2026.02.03-003", shards=1),
        ]
        groups = propose_merges(indices, "monthly")
        assert len(groups) == 1
        assert groups[0].shard_reduction == 2  # 3 shards -> 1


class TestUnionMappings:
    def test_identical_mappings(self):
        m = {"properties": {"field_a": {"type": "keyword"}, "field_b": {"type": "long"}}}
        result, conflicts = union_mappings([m, m])
        assert set(result["properties"].keys()) == {"field_a", "field_b"}
        assert conflicts == []

    def test_field_drift(self):
        m1 = {"properties": {"a": {"type": "keyword"}}}
        m2 = {"properties": {"a": {"type": "keyword"}, "b": {"type": "long"}}}
        result, conflicts = union_mappings([m1, m2])
        assert "a" in result["properties"]
        assert "b" in result["properties"]
        assert conflicts == []

    def test_type_conflict_keeps_first(self):
        m1 = {"properties": {"x": {"type": "keyword"}}}
        m2 = {"properties": {"x": {"type": "text"}}}
        result, conflicts = union_mappings([m1, m2])
        assert result["properties"]["x"]["type"] == "keyword"

    def test_type_conflict_reported(self):
        m1 = {"properties": {"x": {"type": "keyword"}}}
        m2 = {"properties": {"x": {"type": "text"}}}
        result, conflicts = union_mappings([m1, m2])
        assert len(conflicts) == 1
        assert "x" in conflicts[0]
        assert "keyword" in conflicts[0]
        assert "text" in conflicts[0]

    def test_multiple_type_conflicts_all_reported(self):
        m1 = {"properties": {"a": {"type": "keyword"}, "b": {"type": "long"}}}
        m2 = {"properties": {"a": {"type": "text"}, "b": {"type": "integer"}}}
        result, conflicts = union_mappings([m1, m2])
        assert len(conflicts) == 2
        assert result["properties"]["a"]["type"] == "keyword"
        assert result["properties"]["b"]["type"] == "long"

    def test_no_conflict_when_types_match(self):
        m1 = {"properties": {"a": {"type": "keyword"}}}
        m2 = {"properties": {"a": {"type": "keyword"}}}
        _, conflicts = union_mappings([m1, m2])
        assert conflicts == []

    def test_preserves_top_level_keys(self):
        m1 = {"properties": {"a": {"type": "keyword"}}, "dynamic": "strict"}
        m2 = {"properties": {"a": {"type": "keyword"}}}
        result, _ = union_mappings([m1, m2])
        assert result.get("dynamic") == "strict"


class TestMergeGroupFromJob:
    def test_round_trip_from_job_record(self):
        """MergeGroup reconstructed from a job record should have valid metadata."""
        from sharderator.engine.merge_analyzer import MergeGroup
        from sharderator.models.job import JobRecord, JobType

        job = JobRecord(
            index_name="partial-.ds-metrics-cpu-default-2026.02-merged",
            job_type=JobType.MERGE,
        )
        job.expected_doc_count = 50000
        job.source_frozen_indices = ["idx-a", "idx-b", "idx-c"]
        job.enriched_metadata = [
            {"name": "idx-a", "shard_count": 1, "doc_count": 20000,
             "store_size_bytes": 1000000, "snapshot_name": "s1",
             "repository_name": "repo", "original_index_name": "orig-a",
             "is_data_stream_member": True, "data_stream_name": "ds-cpu",
             "has_custom_routing": False, "mappings": {}, "settings": {},
             "ilm_policy": "", "target_shard_count": 1},
            {"name": "idx-b", "shard_count": 1, "doc_count": 15000,
             "store_size_bytes": 800000, "snapshot_name": "s2",
             "repository_name": "repo", "original_index_name": "orig-b",
             "is_data_stream_member": True, "data_stream_name": "ds-cpu",
             "has_custom_routing": False, "mappings": {}, "settings": {},
             "ilm_policy": "", "target_shard_count": 1},
            {"name": "idx-c", "shard_count": 1, "doc_count": 15000,
             "store_size_bytes": 700000, "snapshot_name": "s3",
             "repository_name": "repo", "original_index_name": "orig-c",
             "is_data_stream_member": True, "data_stream_name": "ds-cpu",
             "has_custom_routing": False, "mappings": {}, "settings": {},
             "ilm_policy": "", "target_shard_count": 1},
        ]

        group = MergeGroup.from_job_record(job)
        assert group.base_pattern == "metrics-cpu-default"
        assert group.time_bucket == "2026.02"
        assert len(group.source_indices) == 3
        assert group.total_docs == 50000
        assert group.total_size_bytes == 2500000
        assert group.merged_index_name == job.index_name

    def test_from_job_without_enriched_metadata(self):
        """Fallback: minimal IndexInfo from source names only."""
        from sharderator.engine.merge_analyzer import MergeGroup
        from sharderator.models.job import JobRecord, JobType

        job = JobRecord(
            index_name="partial-.ds-logs-app-2026.Q1-merged",
            job_type=JobType.MERGE,
        )
        job.source_frozen_indices = ["idx-x", "idx-y"]

        group = MergeGroup.from_job_record(job)
        assert group.base_pattern == "logs-app"
        assert group.time_bucket == "2026.Q1"
        assert len(group.source_indices) == 2
        assert group.source_indices[0].name == "idx-x"
        assert group.source_indices[1].name == "idx-y"

    def test_parse_intermediate_name_format(self):
        """Handle the sharderator-merged-* naming format."""
        from sharderator.engine.merge_analyzer import MergeGroup
        from sharderator.models.job import JobRecord, JobType

        job = JobRecord(
            index_name="sharderator-merged-metrics-network-2026.03",
            job_type=JobType.MERGE,
        )
        job.source_frozen_indices = ["a"]

        group = MergeGroup.from_job_record(job)
        assert group.base_pattern == "metrics-network"
        assert group.time_bucket == "2026.03"

    def test_parse_yearly_bucket(self):
        """Yearly bucket (just a year) parses correctly."""
        from sharderator.engine.merge_analyzer import MergeGroup
        from sharderator.models.job import JobRecord, JobType

        job = JobRecord(
            index_name="partial-.ds-logs-server-2026-merged",
            job_type=JobType.MERGE,
        )
        job.source_frozen_indices = ["a", "b"]

        group = MergeGroup.from_job_record(job)
        assert group.base_pattern == "logs-server"
        assert group.time_bucket == "2026"

    def test_empty_source_indices_when_no_metadata(self):
        """Job with no enriched_metadata and no source_frozen_indices."""
        from sharderator.engine.merge_analyzer import MergeGroup
        from sharderator.models.job import JobRecord, JobType

        job = JobRecord(
            index_name="partial-.ds-metrics-host-2026.02-merged",
            job_type=JobType.MERGE,
        )
        # No enriched_metadata, no source_frozen_indices

        group = MergeGroup.from_job_record(job)
        assert group.source_indices == []
        assert group.base_pattern == "metrics-host"
        assert group.time_bucket == "2026.02"


class TestUnionAnalysisSettings:
    def test_analysis_conflict_detected(self):
        from sharderator.engine.merger import _union_analysis_settings

        i1 = IndexInfo(name="a", shard_count=1, doc_count=0, store_size_bytes=0)
        i1.settings = {"analysis": {"analyzer": {
            "my_analyzer": {"type": "custom", "tokenizer": "standard"}
        }}}

        i2 = IndexInfo(name="b", shard_count=1, doc_count=0, store_size_bytes=0)
        i2.settings = {"analysis": {"analyzer": {
            "my_analyzer": {"type": "custom", "tokenizer": "whitespace"}
        }}}

        merged, conflicts = _union_analysis_settings([i1, i2])
        assert merged["analyzer"]["my_analyzer"]["tokenizer"] == "standard"  # first wins
        assert len(conflicts) == 1
        assert "my_analyzer" in conflicts[0]

    def test_analysis_no_conflict_when_identical(self):
        from sharderator.engine.merger import _union_analysis_settings

        i1 = IndexInfo(name="a", shard_count=1, doc_count=0, store_size_bytes=0)
        i1.settings = {"analysis": {"analyzer": {
            "my_analyzer": {"type": "custom", "tokenizer": "standard"}
        }}}

        i2 = IndexInfo(name="b", shard_count=1, doc_count=0, store_size_bytes=0)
        i2.settings = {"analysis": {"analyzer": {
            "my_analyzer": {"type": "custom", "tokenizer": "standard"}
        }}}

        _, conflicts = _union_analysis_settings([i1, i2])
        assert conflicts == []

    def test_analysis_union_adds_new_components(self):
        from sharderator.engine.merger import _union_analysis_settings

        i1 = IndexInfo(name="a", shard_count=1, doc_count=0, store_size_bytes=0)
        i1.settings = {"analysis": {"normalizer": {
            "uppercase": {"type": "custom", "filter": ["uppercase"]}
        }}}

        i2 = IndexInfo(name="b", shard_count=1, doc_count=0, store_size_bytes=0)
        i2.settings = {"analysis": {"normalizer": {
            "lowercase": {"type": "custom", "filter": ["lowercase"]}
        }}}

        merged, conflicts = _union_analysis_settings([i1, i2])
        assert "uppercase" in merged["normalizer"]
        assert "lowercase" in merged["normalizer"]
        assert conflicts == []

    def test_analysis_empty_settings(self):
        from sharderator.engine.merger import _union_analysis_settings

        i1 = IndexInfo(name="a", shard_count=1, doc_count=0, store_size_bytes=0)
        i1.settings = {}

        merged, conflicts = _union_analysis_settings([i1])
        assert merged == {}
        assert conflicts == []
