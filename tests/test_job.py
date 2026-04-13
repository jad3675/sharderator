"""Tests for the job state machine — highest-value unit tests."""

import pytest

from sharderator.models.job import JobRecord, JobState, JobType, TRANSITIONS


class TestTransitions:
    def test_every_valid_transition_succeeds(self):
        """Every declared transition in TRANSITIONS should work."""
        for source, targets in TRANSITIONS.items():
            for target in targets:
                job = JobRecord(index_name="test")
                job.state = source  # force state
                job.transition(target)
                assert job.state == target
                assert len(job.history) == 1
                assert job.history[0]["from"] == source.value
                assert job.history[0]["to"] == target.value

    def test_invalid_transition_raises(self):
        job = JobRecord(index_name="test")
        with pytest.raises(ValueError, match="Invalid transition"):
            job.transition(JobState.COMPLETED)

    def test_pending_to_completed_invalid(self):
        job = JobRecord(index_name="test")
        with pytest.raises(ValueError):
            job.transition(JobState.COMPLETED)

    def test_failed_is_terminal(self):
        job = JobRecord(index_name="test")
        job.state = JobState.FAILED
        with pytest.raises(ValueError):
            job.transition(JobState.COMPLETED)
        with pytest.raises(ValueError):
            job.transition(JobState.PENDING)

    def test_completed_is_terminal(self):
        job = JobRecord(index_name="test")
        job.state = JobState.COMPLETED
        with pytest.raises(ValueError):
            job.transition(JobState.FAILED)

    def test_error_stored_on_failure(self):
        job = JobRecord(index_name="test")
        job.transition(JobState.ANALYZING)
        job.transition(JobState.FAILED, error="something broke")
        assert job.error == "something broke"
        assert job.state == JobState.FAILED


class TestSerialization:
    def test_round_trip(self):
        job = JobRecord(index_name="test-idx", job_type=JobType.MERGE)
        job.transition(JobState.ANALYZING)
        job.expected_doc_count = 42000
        job.restore_indices = ["a", "b"]

        restored = JobRecord.from_dict(job.to_dict())
        assert restored.index_name == "test-idx"
        assert restored.job_type == JobType.MERGE
        assert restored.state == JobState.ANALYZING
        assert restored.expected_doc_count == 42000
        assert restored.restore_indices == ["a", "b"]
        assert len(restored.history) == 1

    def test_unknown_job_type_defaults_to_shrink(self):
        data = {"index_name": "x", "state": "PENDING", "job_type": "UNKNOWN"}
        job = JobRecord.from_dict(data)
        assert job.job_type == JobType.SHRINK

    def test_missing_fields_have_defaults(self):
        data = {"index_name": "x", "state": "PENDING"}
        job = JobRecord.from_dict(data)
        assert job.job_type == JobType.SHRINK
        assert job.expected_doc_count == 0
        assert job.enriched_metadata == []
