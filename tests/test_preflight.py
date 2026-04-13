"""Tests for pre-flight cluster health checks."""

from unittest.mock import MagicMock

import pytest

from sharderator.engine.preflight import (
    ClusterNotHealthyError,
    check_circuit_breakers,
    check_cluster_health,
)


def _mock_client(health_status="green", relocating=0, initializing=0, unassigned=0):
    client = MagicMock()
    client.cluster.health.return_value = {
        "status": health_status,
        "relocating_shards": relocating,
        "initializing_shards": initializing,
        "unassigned_shards": unassigned,
    }
    return client


class TestClusterHealth:
    def test_green_passes(self):
        check_cluster_health(_mock_client("green"))

    def test_yellow_passes_when_allowed(self):
        check_cluster_health(_mock_client("yellow"), allow_yellow=True)

    def test_yellow_blocked_when_not_allowed(self):
        with pytest.raises(ClusterNotHealthyError, match="YELLOW"):
            check_cluster_health(_mock_client("yellow"), allow_yellow=False)

    def test_red_always_blocked(self):
        with pytest.raises(ClusterNotHealthyError, match="RED"):
            check_cluster_health(_mock_client("red"))

    def test_high_relocating_blocked(self):
        with pytest.raises(ClusterNotHealthyError, match="relocating"):
            check_cluster_health(_mock_client("green", relocating=15))

    def test_high_initializing_blocked(self):
        with pytest.raises(ClusterNotHealthyError, match="initializing"):
            check_cluster_health(_mock_client("green", initializing=12))


class TestCircuitBreakers:
    def test_under_threshold_passes(self):
        client = MagicMock()
        client.nodes.stats.return_value = {
            "nodes": {
                "node1": {
                    "name": "node1",
                    "breakers": {
                        "parent": {
                            "limit_size_in_bytes": 1000,
                            "estimated_size_in_bytes": 790,
                        }
                    },
                }
            }
        }
        check_circuit_breakers(client)  # 79% — should pass

    def test_over_threshold_blocked(self):
        client = MagicMock()
        client.nodes.stats.return_value = {
            "nodes": {
                "node1": {
                    "name": "node1",
                    "breakers": {
                        "parent": {
                            "limit_size_in_bytes": 1000,
                            "estimated_size_in_bytes": 810,
                        }
                    },
                }
            }
        }
        with pytest.raises(ClusterNotHealthyError, match="Circuit breaker"):
            check_circuit_breakers(client)  # 81% — should block
