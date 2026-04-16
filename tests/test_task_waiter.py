"""Tests for async task polling and transient failure classification."""

from unittest.mock import MagicMock, patch

import pytest

from sharderator.engine.task_waiter import (
    TRANSIENT_FAILURE_TYPES,
    PermanentReindexError,
    TransientReindexError,
    _is_transient_failure,
    wait_for_task,
)


class TestTransientClassification:
    """Verify _is_transient_failure correctly classifies error types."""

    @pytest.mark.parametrize("err_type", sorted(TRANSIENT_FAILURE_TYPES))
    def test_all_transient_types_recognized(self, err_type):
        """Every entry in TRANSIENT_FAILURE_TYPES should return True."""
        failure = {"cause": {"type": err_type, "reason": "test"}}
        assert _is_transient_failure(failure) is True

    def test_search_context_missing_is_transient(self):
        failure = {
            "cause": {
                "type": "search_context_missing_exception",
                "reason": "No search context found for id [12345]",
            }
        }
        assert _is_transient_failure(failure) is True

    def test_search_phase_execution_is_transient(self):
        failure = {
            "cause": {
                "type": "search_phase_execution_exception",
                "reason": "all shards failed",
            }
        }
        assert _is_transient_failure(failure) is True

    def test_nested_transient_in_caused_by(self):
        """Transient type in nested caused_by should also return True."""
        failure = {
            "cause": {
                "type": "some_wrapper_exception",
                "reason": "wrapper",
                "caused_by": {
                    "type": "search_context_missing_exception",
                    "reason": "No search context found",
                },
            }
        }
        assert _is_transient_failure(failure) is True

    def test_permanent_type_returns_false(self):
        failure = {
            "cause": {
                "type": "mapper_parsing_exception",
                "reason": "failed to parse field",
            }
        }
        assert _is_transient_failure(failure) is False

    def test_empty_cause_returns_false(self):
        assert _is_transient_failure({}) is False
        assert _is_transient_failure({"cause": {}}) is False


class TestWaitForTaskFailureClassification:
    """Verify wait_for_task raises the correct error type based on failure classification."""

    @patch("sharderator.engine.task_waiter.time.sleep")
    def test_transient_failures_raise_transient_error(self, mock_sleep):
        """All scroll-expiry failures should raise TransientReindexError."""
        client = MagicMock()
        client.tasks.get.return_value = {
            "completed": True,
            "response": {
                "total": 500000,
                "created": 292000,
                "failures": [
                    {
                        "cause": {
                            "type": "search_context_missing_exception",
                            "reason": "No search context found for id [abc123]",
                        }
                    },
                    {
                        "cause": {
                            "type": "search_context_missing_exception",
                            "reason": "No search context found for id [def456]",
                        }
                    },
                ],
            },
        }

        with pytest.raises(TransientReindexError) as exc_info:
            wait_for_task(client, "task-1")

        assert exc_info.value.failure_count == 2
        assert exc_info.value.created == 292000
        assert exc_info.value.total == 500000

    @patch("sharderator.engine.task_waiter.time.sleep")
    def test_mixed_failures_raise_permanent_error(self, mock_sleep):
        """Mix of transient and permanent failures should raise PermanentReindexError."""
        client = MagicMock()
        client.tasks.get.return_value = {
            "completed": True,
            "response": {
                "total": 100,
                "created": 90,
                "failures": [
                    {
                        "cause": {
                            "type": "search_context_missing_exception",
                            "reason": "scroll expired",
                        }
                    },
                    {
                        "cause": {
                            "type": "mapper_parsing_exception",
                            "reason": "failed to parse",
                        }
                    },
                ],
            },
        }

        with pytest.raises(PermanentReindexError):
            wait_for_task(client, "task-1")

    @patch("sharderator.engine.task_waiter.time.sleep")
    def test_no_failures_returns_normally(self, mock_sleep):
        """Successful task with no failures should return without error."""
        client = MagicMock()
        client.tasks.get.return_value = {
            "completed": True,
            "response": {
                "total": 1000,
                "created": 1000,
                "failures": [],
            },
        }

        # Should not raise
        wait_for_task(client, "task-1")
