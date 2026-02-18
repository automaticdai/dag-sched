"""DAG task data structure with factory methods."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class DAGTask:
    """A Directed Acyclic Graph task with execution time metadata.

    Parameters
    ----------
    successors : dict mapping node_id -> list of successor node_ids
    wcet : dict mapping node_id -> worst-case execution time
    """

    def __init__(self, successors: dict[int, list[int]], wcet: dict[int, int]) -> None:
        self.successors = {k: list(v) for k, v in successors.items()}
        self.wcet = dict(wcet)
        self.vertices = sorted(self.successors.keys())
        self.predecessors = self._build_predecessors()
        self._validate()

    @property
    def source(self) -> int:
        sources = [v for v in self.vertices if not self.predecessors[v]]
        return sources[0]

    @property
    def sink(self) -> int:
        sinks = [v for v in self.vertices if not self.successors[v]]
        return sinks[0]

    def _build_predecessors(self) -> dict[int, list[int]]:
        pre: dict[int, list[int]] = {v: [] for v in self.vertices}
        for node, succs in self.successors.items():
            for s in succs:
                pre[s].append(node)
        return pre

    def _validate(self) -> None:
        for v in self.vertices:
            if v not in self.wcet:
                raise ValueError(f"Missing wcet for node {v}")

        # Cycle detection first (via Kahn's algorithm / topological sort).
        # This must run before source/sink checks because a cycle can cause
        # zero sources or zero sinks, and the error should report the cycle.
        in_degree = {v: len(self.predecessors[v]) for v in self.vertices}
        queue = [v for v in self.vertices if in_degree[v] == 0]
        visited = 0
        while queue:
            node = queue.pop(0)
            visited += 1
            for s in self.successors.get(node, []):
                in_degree[s] -= 1
                if in_degree[s] == 0:
                    queue.append(s)
        if visited != len(self.vertices):
            raise ValueError("Graph contains a cycle")

        sources = [v for v in self.vertices if not self.predecessors[v]]
        if len(sources) != 1:
            raise ValueError(f"Expected exactly 1 source node, found {len(sources)}: {sources}")

        sinks = [v for v in self.vertices if not self.successors[v]]
        if len(sinks) != 1:
            raise ValueError(f"Expected exactly 1 sink node, found {len(sinks)}: {sinks}")

    @staticmethod
    def builder() -> DAGTaskBuilder:
        return DAGTaskBuilder()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DAGTask:
        nodes = {int(k): int(v) for k, v in data["nodes"].items()}
        successors: dict[int, list[int]] = {nid: [] for nid in nodes}
        for src, dst in data["edges"]:
            successors[int(src)].append(int(dst))
        return cls(successors, nodes)

    @classmethod
    def from_json(cls, path: str) -> DAGTask:
        with open(path) as f:
            data = json.load(f)
        return cls.from_dict(data)

    @classmethod
    def from_yaml(cls, path: str) -> DAGTask:
        import yaml
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls.from_dict(data)


class DAGTaskBuilder:
    def __init__(self) -> None:
        self._nodes: dict[int, int] = {}
        self._edges: list[tuple[int, int]] = []

    def add_node(self, node_id: int, wcet: int) -> DAGTaskBuilder:
        self._nodes[node_id] = wcet
        return self

    def add_edge(self, src: int, dst: int) -> DAGTaskBuilder:
        self._edges.append((src, dst))
        return self

    def build(self) -> DAGTask:
        successors: dict[int, list[int]] = {nid: [] for nid in self._nodes}
        for src, dst in self._edges:
            successors[src].append(dst)
        return DAGTask(successors, self._nodes)
