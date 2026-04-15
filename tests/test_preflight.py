"""Tests for pre-flight cluster health checks and circuit breaker backpressure."""

from unittest.mock import MagicMock, patch, call

import pytest

from sharderator.engine.preflight import (
    ClusterNotHealthyError,
    _get_hot_breakers,
    check_circuit_breakers,
    check_circuit_breakers_instant,
    check_cluster_health,
    _check_cluster_pressure,
    wait_for_cluster_ready,
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


def _breaker_stats(breakers_by_node: dict[str, dict[str, tuple[int, int]]]):
    """Build a nodes.stats(metric="breaker") response.

    breakers_by_node: {node_name: {breaker_name: (limit, estimated)}}
    """
    nodes = {}
    for node_name, breakers in breakers_by_node.items():
        nodes[node_name] = {
            "name": node_name,
            "breakers": {
                name: {
                    "limit_size_in_bytes": limit,
                    "estimated_size_in_bytes": estimated,
                }
                for name, (limit, estimated) in breakers.items()
            },
        }
    return {"nodes": nodes}


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


class TestGetHotBreakers:
    def test_empty_when_all_below_threshold(self):
        client = MagicMock()
        client.nodes.stats.return_value = _breaker_stats({
            "node1": {"parent": (1000, 790)},  # 79%
        })
        assert _get_hot_breakers(client, threshold=0.80) == []

    def test_returns_hot_breaker(self):
        client = MagicMock()
        client.nodes.stats.return_value = _breaker_stats({
            "node1": {"parent": (1000, 850)},  # 85%
        })
        hot = _get_hot_breakers(client, threshold=0.80)
        assert len(hot) == 1
        assert hot[0][0] == "parent"
        assert hot[0][1] == "node1"
        assert hot[0][2] == pytest.approx(0.85)

    def test_multiple_hot_breakers_across_nodes(self):
        client = MagicMock()
        client.nodes.stats.return_value = _breaker_stats({
            "node1": {"parent": (1000, 850), "fielddata": (500, 200)},  # parent hot
            "node2": {"parent": (1000, 900)},  # also hot
        })
        hot = _get_hot_breakers(client, threshold=0.80)
        assert len(hot) == 2
        names = {(b[0], b[1]) for b in hot}
        assert ("parent", "node1") in names
        assert ("parent", "node2") in names

    def test_zero_limit_ignored(self):
        client = MagicMock()
        client.nodes.stats.return_value = _breaker_stats({
            "node1": {"parent": (0, 100)},  # limit=0, skip
        })
        assert _get_hot_breakers(client, threshold=0.80) == []

    def test_custom_threshold(self):
        client = MagicMock()
        client.nodes.stats.return_value = _breaker_stats({
            "node1": {"parent": (1000, 760)},  # 76%
        })
        # At 80% threshold, not hot
        assert _get_hot_breakers(client, threshold=0.80) == []
        # At 75% threshold, hot
        hot = _get_hot_breakers(client, threshold=0.75)
        assert len(hot) == 1


class TestCheckCircuitBreakers:
    def test_under_threshold_passes(self):
        client = MagicMock()
        client.nodes.stats.return_value = _breaker_stats({
            "node1": {"parent": (1000, 790)},
        })
        check_circuit_breakers(client)  # 79% — should pass

    def test_over_threshold_fail_fast(self):
        """With wait_timeout_minutes=0, should fail immediately."""
        client = MagicMock()
        client.nodes.stats.return_value = _breaker_stats({
            "node1": {"parent": (1000, 810)},
        })
        with pytest.raises(ClusterNotHealthyError, match="Circuit breaker"):
            check_circuit_breakers(client, wait_timeout_minutes=0)

    @patch("sharderator.engine.preflight.time.sleep")
    def test_waits_then_clears(self, mock_sleep):
        """Breaker hot on first two checks, then clears."""
        client = MagicMock()
        hot_stats = _breaker_stats({"node1": {"parent": (1000, 820)}})
        clear_stats = _breaker_stats({"node1": {"parent": (1000, 700)}})
        client.nodes.stats.side_effect = [hot_stats, hot_stats, clear_stats]

        # Should return without raising
        check_circuit_breakers(client, wait_timeout_minutes=10)
        assert mock_sleep.call_count >= 1

    @patch("sharderator.engine.preflight.time.time")
    @patch("sharderator.engine.preflight.time.sleep")
    def test_waits_then_fails(self, mock_sleep, mock_time):
        """Breaker stays hot past timeout — should raise."""
        client = MagicMock()
        hot_stats = _breaker_stats({"node1": {"parent": (1000, 850)}})
        client.nodes.stats.return_value = hot_stats

        # time.time() is called multiple times:
        #   1. deadline = time.time() + timeout*60  (returns 0 → deadline=600)
        #   2. while time.time() < deadline         (returns 0 → enters loop)
        #   3. while time.time() < deadline         (returns 700 → exits loop)
        # Plus additional calls inside the loop for logging.
        # Use a counter to return 0 for the first few, then jump past deadline.
        call_count = [0]
        def fake_time():
            call_count[0] += 1
            if call_count[0] <= 3:
                return 0
            return 700  # past deadline of 600

        mock_time.side_effect = fake_time

        with pytest.raises(ClusterNotHealthyError, match="sustained memory pressure"):
            check_circuit_breakers(client, wait_timeout_minutes=10)


class TestCheckCircuitBreakersInstant:
    def test_no_issues_when_clear(self):
        client = MagicMock()
        client.nodes.stats.return_value = _breaker_stats({
            "node1": {"parent": (1000, 500)},
        })
        assert check_circuit_breakers_instant(client) == []

    def test_returns_issues_without_sleeping(self):
        client = MagicMock()
        client.nodes.stats.return_value = _breaker_stats({
            "node1": {"parent": (1000, 850)},
        })
        with patch("sharderator.engine.preflight.time.sleep") as mock_sleep:
            issues = check_circuit_breakers_instant(client)
            assert len(issues) == 1
            assert "85%" in issues[0]
            mock_sleep.assert_not_called()


class TestCheckClusterPressure:
    def test_includes_breaker_issues(self):
        """_check_cluster_pressure should report circuit breakers at 75% threshold."""
        client = MagicMock()
        client.cluster.pending_tasks.return_value = {"tasks": []}
        client.cluster.health.return_value = {
            "status": "green",
            "relocating_shards": 0,
            "initializing_shards": 0,
        }
        # nodes.stats is called twice: once for thread_pool,jvm and once for breaker
        # via _get_hot_breakers. We need to handle both calls.
        tp_jvm_stats = {
            "nodes": {
                "node1": {
                    "name": "node1",
                    "thread_pool": {"write": {"queue": 0}, "search": {"queue": 0}, "snapshot": {"queue": 0}},
                    "jvm": {"mem": {"heap_used_in_bytes": 500, "heap_max_in_bytes": 1000}},
                }
            }
        }
        breaker_stats = _breaker_stats({"node1": {"parent": (1000, 760)}})  # 76% > 75%
        client.nodes.stats.side_effect = [tp_jvm_stats, breaker_stats]

        issues = _check_cluster_pressure(client)
        breaker_issues = [i for i in issues if "Circuit breaker" in i]
        assert len(breaker_issues) == 1
        assert "76%" in breaker_issues[0]

    def test_no_issues_when_clear(self):
        client = MagicMock()
        client.cluster.pending_tasks.return_value = {"tasks": []}
        client.cluster.health.return_value = {
            "status": "green",
            "relocating_shards": 0,
            "initializing_shards": 0,
        }
        tp_jvm_stats = {
            "nodes": {
                "node1": {
                    "name": "node1",
                    "thread_pool": {"write": {"queue": 0}, "search": {"queue": 0}, "snapshot": {"queue": 0}},
                    "jvm": {"mem": {"heap_used_in_bytes": 500, "heap_max_in_bytes": 1000}},
                }
            }
        }
        breaker_stats = _breaker_stats({"node1": {"parent": (1000, 700)}})  # 70% < 75%
        client.nodes.stats.side_effect = [tp_jvm_stats, breaker_stats]

        issues = _check_cluster_pressure(client)
        assert issues == []


class TestWaitForClusterReady:
    def test_no_pressure_returns_immediately(self):
        """All clear — should return with no sleep."""
        client = _mock_client("green")
        client.cluster.pending_tasks.return_value = {"tasks": []}
        tp_jvm_stats = {
            "nodes": {
                "node1": {
                    "name": "node1",
                    "thread_pool": {"write": {"queue": 0}, "search": {"queue": 0}, "snapshot": {"queue": 0}},
                    "jvm": {"mem": {"heap_used_in_bytes": 400, "heap_max_in_bytes": 1000}},
                }
            }
        }
        breaker_stats = _breaker_stats({"node1": {"parent": (1000, 600)}})
        client.nodes.stats.side_effect = [tp_jvm_stats, breaker_stats]

        with patch("sharderator.engine.preflight.time.sleep") as mock_sleep:
            wait_for_cluster_ready(client)
            mock_sleep.assert_not_called()

    @patch("sharderator.engine.preflight.time.sleep")
    def test_waits_for_breaker_then_clears(self, mock_sleep):
        """Pressure present then clears — should sleep then return."""
        client = _mock_client("green")
        client.cluster.pending_tasks.return_value = {"tasks": []}

        tp_jvm_clear = {
            "nodes": {
                "node1": {
                    "name": "node1",
                    "thread_pool": {"write": {"queue": 0}, "search": {"queue": 0}, "snapshot": {"queue": 0}},
                    "jvm": {"mem": {"heap_used_in_bytes": 400, "heap_max_in_bytes": 1000}},
                }
            }
        }
        breaker_hot = _breaker_stats({"node1": {"parent": (1000, 780)}})  # 78% > 75%
        breaker_clear = _breaker_stats({"node1": {"parent": (1000, 600)}})

        # First _check_cluster_pressure: hot. Second: clear.
        client.nodes.stats.side_effect = [
            tp_jvm_clear, breaker_hot,    # first pressure check — hot
            tp_jvm_clear, breaker_clear,  # second pressure check — clear
        ]

        wait_for_cluster_ready(client)
        assert mock_sleep.call_count >= 1

    def test_disabled_when_timeout_zero(self):
        """With wait_timeout_minutes=0, should return immediately."""
        client = _mock_client("green")
        with patch("sharderator.engine.preflight.time.sleep") as mock_sleep:
            wait_for_cluster_ready(client, wait_timeout_minutes=0)
            mock_sleep.assert_not_called()
