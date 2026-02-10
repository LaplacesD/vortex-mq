"""Tests for cluster node discovery."""

from __future__ import annotations

import pytest

from vortex.cluster import ClusterDiscovery, ClusterNode


class TestClusterDiscovery:
    def test_register_self(self):
        cd = ClusterDiscovery(node_id="node-1")
        node = cd.register_self("localhost", 9090)
        assert node.node_id == "node-1"
        assert node.host == "localhost"
        assert node.port == 9090

    def test_add_and_get_node(self):
        cd = ClusterDiscovery()
        node = ClusterNode(node_id="node-2", host="10.0.0.2", port=9091)
        cd.add_node(node)
        assert cd.get_node("node-2") is node

    def test_remove_node(self):
        cd = ClusterDiscovery()
        cd.add_node(ClusterNode(node_id="node-3", host="10.0.0.3", port=9092))
        cd.remove_node("node-3")
        assert cd.get_node("node-3") is None

    def test_get_all_nodes(self):
        cd = ClusterDiscovery()
        cd.register_self("localhost", 9090)
        cd.add_node(ClusterNode(node_id="n1", host="h1", port=1))
        cd.add_node(ClusterNode(node_id="n2", host="h2", port=2))
        assert len(cd.get_all_nodes()) == 3

    def test_get_active_nodes(self):
        cd = ClusterDiscovery(node_id="self")
        cd.register_self("localhost", 1)
        cd.add_node(ClusterNode(node_id="alive", host="h1", port=2))
        import time
        dead = ClusterNode(node_id="dead", host="h2", port=3)
        dead.last_heartbeat = time.time() - 100
        cd.add_node(dead)

        active = cd.get_active_nodes()
        assert all(n.status == "active" for n in active)
        assert len(active) == 2
        assert "dead" not in [n.node_id for n in active]
