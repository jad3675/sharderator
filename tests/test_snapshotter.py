"""Tests for snapshotter — force merge task detection."""

from sharderator.engine.snapshotter import _is_forcemerge_for_index


class TestIsForcemergeForIndex:
    def test_exact_match_in_brackets(self):
        task = {"description": "Force-merge indices [logs-1], maxSegments[1]"}
        assert _is_forcemerge_for_index(task, "logs-1") is True

    def test_substring_does_not_match(self):
        """'logs-1' must not match a task running on 'logs-10'."""
        task = {"description": "Force-merge indices [logs-1], maxSegments[1]"}
        assert _is_forcemerge_for_index(task, "logs-10") is False
        assert _is_forcemerge_for_index(task, "logs") is False

    def test_longer_name_does_not_match_shorter(self):
        task = {"description": "Force-merge indices [logs-10], maxSegments[1]"}
        assert _is_forcemerge_for_index(task, "logs-1") is False
        assert _is_forcemerge_for_index(task, "logs-10") is True

    def test_empty_description(self):
        assert _is_forcemerge_for_index({}, "logs-1") is False
        assert _is_forcemerge_for_index({"description": ""}, "logs-1") is False

    def test_no_description_key(self):
        assert _is_forcemerge_for_index({"action": "indices:admin/forcemerge"}, "logs-1") is False

    def test_numeric_suffix_variants(self):
        """Common real-world pattern: metrics-host-1 vs metrics-host-10."""
        task1 = {"description": "Force-merge indices [metrics-host-1], maxSegments[1]"}
        task10 = {"description": "Force-merge indices [metrics-host-10], maxSegments[1]"}

        assert _is_forcemerge_for_index(task1, "metrics-host-1") is True
        assert _is_forcemerge_for_index(task1, "metrics-host-10") is False
        assert _is_forcemerge_for_index(task10, "metrics-host-1") is False
        assert _is_forcemerge_for_index(task10, "metrics-host-10") is True

    def test_sharderator_prefixed_index(self):
        """Sharderator's own intermediate index names."""
        task = {"description": "Force-merge indices [sharderator-shrunk-metrics-cpu-2026.02], maxSegments[1]"}
        assert _is_forcemerge_for_index(task, "sharderator-shrunk-metrics-cpu-2026.02") is True
        assert _is_forcemerge_for_index(task, "sharderator-shrunk-metrics-cpu-2026.0") is False
