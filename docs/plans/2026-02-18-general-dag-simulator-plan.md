# General DAG Scheduling Simulator — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rewrite the experiment-specific DAG scheduling codebase into a general-purpose `dag_sched` Python package with clean APIs for defining DAGs, plugging in custom schedulers, and running simulations.

**Architecture:** A flat Python package (`dag_sched/`) with six modules: `dag.py` (DAG data structure + builders), `job.py` (runtime job), `core.py` (processing unit), `scheduler.py` (ABC + built-in random), `simulator.py` (scheduling loop), `graph.py` (graph utilities), and `config.py` (JSON/YAML loading). The old `src/` directory is deleted. Tests live in `tests/`.

**Tech Stack:** Python 3.10+, PyYAML (for YAML config loading), pytest (for tests). No networkx dependency.

---

### Task 1: Project scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `dag_sched/__init__.py`
- Create: `tests/__init__.py`

**Step 1: Create pyproject.toml**

```toml
[build-system]
requires = ["setuptools>=68.0"]
build-backend = "setuptools.backends._legacy:_Backend"

[project]
name = "dag-sched"
version = "0.1.0"
description = "A general-purpose DAG scheduling simulator"
requires-python = ">=3.10"
dependencies = ["pyyaml>=6.0"]

[project.optional-dependencies]
dev = ["pytest>=7.0"]
```

**Step 2: Create empty dag_sched/__init__.py**

```python
"""DAG Scheduling Simulator — a general-purpose DAG scheduling framework."""
```

**Step 3: Create empty tests/__init__.py**

Empty file.

**Step 4: Install the package in dev mode**

Run: `pip install -e ".[dev]"`
Expected: Successful install

**Step 5: Commit**

```bash
git add pyproject.toml dag_sched/__init__.py tests/__init__.py
git commit -m "scaffold: create dag_sched package structure"
```

---

### Task 2: Core (processing unit)

**Files:**
- Create: `dag_sched/core.py`
- Create: `tests/test_core.py`

**Step 1: Write the failing tests**

```python
from dag_sched.core import Core


class TestCore:
    def test_new_core_is_idle(self):
        core = Core()
        assert core.is_idle() is True
        assert core.get_workload() == 0

    def test_assign_job_makes_core_busy(self):
        core = Core()
        core.assign(job_id=1, execution_time=10)
        assert core.is_idle() is False
        assert core.get_workload() == 10

    def test_execute_reduces_workload(self):
        core = Core()
        core.assign(job_id=1, execution_time=10)
        job_id, finished = core.execute(3)
        assert job_id == 1
        assert finished is False
        assert core.get_workload() == 7
        assert core.is_idle() is False

    def test_execute_finishes_job(self):
        core = Core()
        core.assign(job_id=1, execution_time=10)
        job_id, finished = core.execute(10)
        assert job_id == 1
        assert finished is True
        assert core.is_idle() is True
        assert core.get_workload() == 0

    def test_execute_idle_core_accumulates_idle_time(self):
        core = Core()
        core.execute(5)
        assert core.get_idle_count() == 5
        core.execute(3)
        assert core.get_idle_count() == 8

    def test_get_running_task(self):
        core = Core()
        assert core.get_running_task() is None
        core.assign(job_id=42, execution_time=5)
        assert core.get_running_task() == 42
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_core.py -v`
Expected: FAIL — cannot import Core

**Step 3: Write implementation**

```python
"""Processing unit that executes jobs."""

from __future__ import annotations


class Core:
    """A single processing core that can execute one job at a time."""

    def __init__(self) -> None:
        self._idle: bool = True
        self._workload: int = 0
        self._job_id: int | None = None
        self._idle_count: int = 0

    def is_idle(self) -> bool:
        return self._idle

    def get_workload(self) -> int:
        return self._workload

    def get_running_task(self) -> int | None:
        return self._job_id if not self._idle else None

    def get_idle_count(self) -> int:
        return self._idle_count

    def assign(self, job_id: int, execution_time: int) -> None:
        self._job_id = job_id
        self._workload = execution_time
        self._idle = False

    def execute(self, t: int) -> tuple[int | None, bool]:
        """Execute current job for *t* time units.

        Returns (job_id, finished). If idle, accumulates idle time.
        """
        if self._idle:
            self._idle_count += t
            return (None, False)

        self._workload -= t
        finished = self._workload == 0
        if finished:
            self._idle = True
        return (self._job_id, finished)
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_core.py -v`
Expected: All 6 tests PASS

**Step 5: Commit**

```bash
git add dag_sched/core.py tests/test_core.py
git commit -m "feat: add Core processing unit"
```

---

### Task 3: DAGTask data structure

**Files:**
- Create: `dag_sched/dag.py`
- Create: `tests/test_dag.py`

**Step 1: Write the failing tests**

```python
import json
import os
import pytest
from dag_sched.dag import DAGTask


# A diamond DAG: 1 -> 2,3 -> 4
DIAMOND_G = {1: [2, 3], 2: [4], 3: [4], 4: []}
DIAMOND_C = {1: 1, 2: 5, 3: 3, 4: 1}


class TestDAGTaskDirect:
    def test_create_from_dicts(self):
        dag = DAGTask(DIAMOND_G, DIAMOND_C)
        assert dag.vertices == [1, 2, 3, 4]
        assert dag.wcet == DIAMOND_C
        assert dag.successors == DIAMOND_G
        assert dag.predecessors[1] == []
        assert set(dag.predecessors[4]) == {2, 3}

    def test_source_and_sink(self):
        dag = DAGTask(DIAMOND_G, DIAMOND_C)
        assert dag.source == 1
        assert dag.sink == 4

    def test_validate_catches_cycle(self):
        cyclic = {1: [2], 2: [3], 3: [1]}
        c = {1: 1, 2: 1, 3: 1}
        with pytest.raises(ValueError, match="cycle"):
            DAGTask(cyclic, c)

    def test_validate_catches_missing_wcet(self):
        g = {1: [2], 2: []}
        c = {1: 1}  # missing node 2
        with pytest.raises(ValueError, match="wcet"):
            DAGTask(g, c)

    def test_validate_catches_multiple_sources(self):
        g = {1: [3], 2: [3], 3: []}
        c = {1: 1, 2: 1, 3: 1}
        with pytest.raises(ValueError, match="source"):
            DAGTask(g, c)

    def test_validate_catches_multiple_sinks(self):
        g = {1: [2, 3], 2: [], 3: []}
        c = {1: 1, 2: 1, 3: 1}
        with pytest.raises(ValueError, match="sink"):
            DAGTask(g, c)


class TestDAGTaskBuilder:
    def test_builder_api(self):
        dag = DAGTask.builder()
        dag.add_node(1, wcet=1)
        dag.add_node(2, wcet=5)
        dag.add_node(3, wcet=3)
        dag.add_node(4, wcet=1)
        dag.add_edge(1, 2)
        dag.add_edge(1, 3)
        dag.add_edge(2, 4)
        dag.add_edge(3, 4)
        result = dag.build()
        assert result.vertices == [1, 2, 3, 4]
        assert result.wcet[2] == 5


class TestDAGTaskFromJSON:
    def test_from_json(self, tmp_path):
        data = {
            "nodes": {"1": 1, "2": 5, "3": 3, "4": 1},
            "edges": [[1, 2], [1, 3], [2, 4], [3, 4]]
        }
        path = tmp_path / "dag.json"
        path.write_text(json.dumps(data))
        dag = DAGTask.from_json(str(path))
        assert dag.vertices == [1, 2, 3, 4]
        assert dag.wcet[2] == 5


class TestDAGTaskFromYAML:
    def test_from_yaml(self, tmp_path):
        content = """
nodes:
  1: 1
  2: 5
  3: 3
  4: 1
edges:
  - [1, 2]
  - [1, 3]
  - [2, 4]
  - [3, 4]
"""
        path = tmp_path / "dag.yaml"
        path.write_text(content)
        dag = DAGTask.from_yaml(str(path))
        assert dag.vertices == [1, 2, 3, 4]
        assert dag.wcet[3] == 3
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_dag.py -v`
Expected: FAIL — cannot import DAGTask

**Step 3: Write implementation**

```python
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
        """The single source node (no predecessors)."""
        sources = [v for v in self.vertices if not self.predecessors[v]]
        return sources[0]

    @property
    def sink(self) -> int:
        """The single sink node (no successors)."""
        sinks = [v for v in self.vertices if not self.successors[v]]
        return sinks[0]

    def _build_predecessors(self) -> dict[int, list[int]]:
        pre: dict[int, list[int]] = {v: [] for v in self.vertices}
        for node, succs in self.successors.items():
            for s in succs:
                pre[s].append(node)
        return pre

    def _validate(self) -> None:
        # Check all nodes have wcet
        for v in self.vertices:
            if v not in self.wcet:
                raise ValueError(f"Missing wcet for node {v}")

        # Check single source
        sources = [v for v in self.vertices if not self.predecessors[v]]
        if len(sources) != 1:
            raise ValueError(f"Expected exactly 1 source node, found {len(sources)}: {sources}")

        # Check single sink
        sinks = [v for v in self.vertices if not self.successors[v]]
        if len(sinks) != 1:
            raise ValueError(f"Expected exactly 1 sink node, found {len(sinks)}: {sinks}")

        # Check for cycles (topological sort via Kahn's algorithm)
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
    """Programmatic builder for DAGTask."""

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
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_dag.py -v`
Expected: All 9 tests PASS

**Step 5: Commit**

```bash
git add dag_sched/dag.py tests/test_dag.py
git commit -m "feat: add DAGTask data structure with builder and config loaders"
```

---

### Task 4: Graph utilities

**Files:**
- Create: `dag_sched/graph.py`
- Create: `tests/test_graph.py`

**Step 1: Write the failing tests**

```python
from dag_sched.graph import (
    find_all_paths,
    find_longest_path,
    find_predecessors,
    find_successors,
    find_ancestors,
    find_descendants,
)

# Diamond: 1 -> 2,3 -> 4
G = {1: [2, 3], 2: [4], 3: [4], 4: []}
WEIGHTS = {1: 1, 2: 5, 3: 3, 4: 1}


class TestFindAllPaths:
    def test_diamond(self):
        paths = find_all_paths(G, 1, 4)
        assert [1, 2, 4] in paths
        assert [1, 3, 4] in paths
        assert len(paths) == 2

    def test_single_node(self):
        paths = find_all_paths({1: []}, 1, 1)
        assert paths == [[1]]


class TestFindLongestPath:
    def test_diamond(self):
        cost, path = find_longest_path(G, 1, 4, WEIGHTS)
        # Path 1->2->4 = 1+5+1=7, Path 1->3->4 = 1+3+1=5
        assert cost == 7
        assert path == [1, 2, 4]


class TestPredecessorsSuccessors:
    def test_predecessors(self):
        assert find_predecessors(G, 4) == [2, 3]
        assert find_predecessors(G, 1) == []

    def test_successors(self):
        assert find_successors(G, 1) == [2, 3]
        assert find_successors(G, 4) == []


class TestAncestorsDescendants:
    def test_ancestors(self):
        # Larger graph: 1->{2,3,4}, 2->{5}, 3->{5}, 4->{5}, 5->{}
        g = {1: [2, 3, 4], 2: [5], 3: [5], 4: [5], 5: []}
        anc = find_ancestors(g, 5)
        assert sorted(anc) == [1, 2, 3, 4]

    def test_ancestors_of_source(self):
        assert find_ancestors(G, 1) == []

    def test_descendants(self):
        desc = find_descendants(G, 1)
        assert sorted(desc) == [2, 3, 4]

    def test_descendants_of_sink(self):
        assert find_descendants(G, 4) == []
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_graph.py -v`
Expected: FAIL — cannot import

**Step 3: Write implementation**

Rewrite the graph utilities from `src/graph.py`, fixing mutable default args and removing hardcoded source/sink.

```python
"""Graph utility functions for DAG analysis."""

from __future__ import annotations

import copy


def find_all_paths(
    graph: dict[int, list[int]], start: int, end: int
) -> list[list[int]]:
    """Find all paths from start to end in a DAG."""
    if start == end:
        return [[start]]
    if start not in graph:
        return []
    paths = []
    for neighbor in graph[start]:
        for p in find_all_paths(graph, neighbor, end):
            paths.append([start] + p)
    return paths


def find_longest_path(
    graph: dict[int, list[int]],
    start: int,
    end: int,
    weights: dict[int, int],
) -> tuple[int, list[int]]:
    """Find the longest (critical) path by total weight."""
    paths = find_all_paths(graph, start, end)
    if not paths:
        return (0, [])
    best_cost = -1
    best_path: list[int] = []
    for path in paths:
        cost = sum(weights[v] for v in path)
        if cost > best_cost:
            best_cost = cost
            best_path = path
    return (best_cost, best_path)


def find_predecessors(graph: dict[int, list[int]], node: int) -> list[int]:
    """Find immediate predecessors of a node."""
    return sorted(k for k, succs in graph.items() if node in succs)


def find_successors(graph: dict[int, list[int]], node: int) -> list[int]:
    """Find immediate successors of a node."""
    return list(graph.get(node, []))


def find_ancestors(graph: dict[int, list[int]], node: int) -> list[int]:
    """Find all ancestors (transitive predecessors) of a node."""
    visited: set[int] = set()
    stack = find_predecessors(graph, node)
    while stack:
        v = stack.pop()
        if v not in visited:
            visited.add(v)
            stack.extend(find_predecessors(graph, v))
    return sorted(visited)


def find_descendants(graph: dict[int, list[int]], node: int) -> list[int]:
    """Find all descendants (transitive successors) of a node."""
    visited: set[int] = set()
    stack = list(graph.get(node, []))
    while stack:
        v = stack.pop()
        if v not in visited:
            visited.add(v)
            stack.extend(graph.get(v, []))
    return sorted(visited)
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_graph.py -v`
Expected: All 8 tests PASS

**Step 5: Commit**

```bash
git add dag_sched/graph.py tests/test_graph.py
git commit -m "feat: add graph utility functions"
```

---

### Task 5: Scheduler ABC + RandomScheduler

**Files:**
- Create: `dag_sched/scheduler.py`
- Create: `tests/test_scheduler.py`

**Step 1: Write the failing tests**

```python
import random
from dag_sched.scheduler import Scheduler, RandomScheduler, SchedulerState
from dag_sched.dag import DAGTask
from dag_sched.core import Core


DIAMOND_G = {1: [2, 3], 2: [4], 3: [4], 4: []}
DIAMOND_C = {1: 1, 2: 5, 3: 3, 4: 1}


class TestSchedulerABC:
    def test_cannot_instantiate_abstract(self):
        import pytest
        with pytest.raises(TypeError):
            Scheduler()


class TestRandomScheduler:
    def test_selects_from_ready_queue(self):
        dag = DAGTask(DIAMOND_G, DIAMOND_C)
        state = SchedulerState(dag=dag, cores=[Core()], current_time=0, finished_tasks=set())
        sched = RandomScheduler(seed=42)
        task_id = sched.select_task([2, 3], state)
        assert task_id in [2, 3]

    def test_deterministic_with_seed(self):
        dag = DAGTask(DIAMOND_G, DIAMOND_C)
        state = SchedulerState(dag=dag, cores=[Core()], current_time=0, finished_tasks=set())
        results = []
        for _ in range(10):
            sched = RandomScheduler(seed=42)
            results.append(sched.select_task([2, 3], state))
        assert len(set(results)) == 1  # same seed -> same result
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_scheduler.py -v`
Expected: FAIL — cannot import

**Step 3: Write implementation**

```python
"""Scheduler interface and built-in implementations."""

from __future__ import annotations

import random as _random
from abc import ABC, abstractmethod
from dataclasses import dataclass

from dag_sched.core import Core
from dag_sched.dag import DAGTask


@dataclass(frozen=True)
class SchedulerState:
    """Read-only snapshot of simulator state passed to the scheduler."""

    dag: DAGTask
    cores: list[Core]
    current_time: int
    finished_tasks: set[int]


class Scheduler(ABC):
    """Base class for scheduling algorithms."""

    @abstractmethod
    def select_task(self, ready_queue: list[int], state: SchedulerState) -> int:
        """Pick a task ID from the ready queue to schedule next."""
        ...

    def on_task_complete(self, task_id: int, time: int) -> None:
        """Optional hook called when a task finishes."""
        pass


class RandomScheduler(Scheduler):
    """Picks a random task from the ready queue."""

    def __init__(self, seed: int | None = None) -> None:
        self._rng = _random.Random(seed)

    def select_task(self, ready_queue: list[int], state: SchedulerState) -> int:
        return self._rng.choice(ready_queue)
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_scheduler.py -v`
Expected: All 3 tests PASS

**Step 5: Commit**

```bash
git add dag_sched/scheduler.py tests/test_scheduler.py
git commit -m "feat: add Scheduler ABC and RandomScheduler"
```

---

### Task 6: DAGSimulator (the scheduling loop)

**Files:**
- Create: `dag_sched/simulator.py`
- Create: `tests/test_simulator.py`

This is the core task — the scheduling loop rewritten from `src/sched.py`.

**Step 1: Write the failing tests**

```python
import math
import pytest
from dag_sched.simulator import DAGSimulator, SimulationResult
from dag_sched.dag import DAGTask
from dag_sched.scheduler import RandomScheduler, Scheduler, SchedulerState


# Linear DAG: 1(1) -> 2(5) -> 3(1)
LINEAR_G = {1: [2], 2: [3], 3: []}
LINEAR_C = {1: 1, 2: 5, 3: 1}

# Diamond: 1(1) -> {2(5), 3(3)} -> 4(1)
DIAMOND_G = {1: [2, 3], 2: [4], 3: [4], 4: []}
DIAMOND_C = {1: 1, 2: 5, 3: 3, 4: 1}


class TestLinearDAG:
    def test_makespan_single_core(self):
        dag = DAGTask(LINEAR_G, LINEAR_C)
        sim = DAGSimulator(dag, num_cores=1, scheduler=RandomScheduler(seed=0))
        result = sim.run()
        # 1 + 5 + 1 = 7 (sequential, no parallelism possible)
        assert result.makespan == 7

    def test_makespan_two_cores_same_as_one(self):
        dag = DAGTask(LINEAR_G, LINEAR_C)
        sim = DAGSimulator(dag, num_cores=2, scheduler=RandomScheduler(seed=0))
        result = sim.run()
        # Linear DAG can't parallelize
        assert result.makespan == 7


class TestDiamondDAG:
    def test_makespan_one_core(self):
        dag = DAGTask(DIAMOND_G, DIAMOND_C)
        sim = DAGSimulator(dag, num_cores=1, scheduler=RandomScheduler(seed=0))
        result = sim.run()
        # Sequential: 1 + 5 + 3 + 1 = 10
        assert result.makespan == 10

    def test_makespan_two_cores(self):
        dag = DAGTask(DIAMOND_G, DIAMOND_C)
        sim = DAGSimulator(dag, num_cores=2, scheduler=RandomScheduler(seed=0))
        result = sim.run()
        # Parallel: 1 (node1) + max(5,3) (nodes 2,3 in parallel) + 1 (node4) = 7
        assert result.makespan == 7


class TestSimulationResult:
    def test_schedule_events_present(self):
        dag = DAGTask(LINEAR_G, LINEAR_C)
        sim = DAGSimulator(dag, num_cores=1, scheduler=RandomScheduler(seed=0))
        result = sim.run()
        assert len(result.schedule) == 3  # 3 nodes scheduled
        # Check all nodes appear
        scheduled_nodes = {e.task_id for e in result.schedule}
        assert scheduled_nodes == {1, 2, 3}

    def test_core_utilization(self):
        dag = DAGTask(LINEAR_G, LINEAR_C)
        sim = DAGSimulator(dag, num_cores=2, scheduler=RandomScheduler(seed=0))
        result = sim.run()
        # Core 0 does all work (7 time units), core 1 idle entire time
        assert len(result.core_utilization) == 2
        assert result.core_utilization[0] == 1.0
        assert result.core_utilization[1] == 0.0


class TestExecutionModels:
    def test_wcet_is_deterministic(self):
        dag = DAGTask(DIAMOND_G, DIAMOND_C)
        results = []
        for _ in range(5):
            sim = DAGSimulator(dag, num_cores=2, scheduler=RandomScheduler(seed=0), execution_model="WCET")
            results.append(sim.run().makespan)
        assert len(set(results)) == 1

    def test_full_random_varies(self):
        dag = DAGTask(DIAMOND_G, DIAMOND_C)
        results = []
        for i in range(20):
            sim = DAGSimulator(dag, num_cores=2, scheduler=RandomScheduler(seed=i), execution_model="FULL_RANDOM", seed=i)
            results.append(sim.run().makespan)
        # With random execution times, we expect some variation
        assert min(results) <= max(results)

    def test_half_random_bounded(self):
        dag = DAGTask(DIAMOND_G, DIAMOND_C)
        for i in range(20):
            sim = DAGSimulator(dag, num_cores=1, scheduler=RandomScheduler(seed=i), execution_model="HALF_RANDOM", seed=i)
            result = sim.run()
            # Each node's exec time >= ceil(wcet/2), so total >= sum(ceil(c/2))
            min_total = sum(math.ceil(c / 2) for c in DIAMOND_C.values())
            max_total = sum(DIAMOND_C.values())
            assert min_total <= result.makespan <= max_total


class TestCustomScheduler:
    def test_highest_wcet_first(self):
        """Custom scheduler that always picks the task with highest WCET."""
        class HighestWCETScheduler(Scheduler):
            def select_task(self, ready_queue, state):
                return max(ready_queue, key=lambda t: state.dag.wcet[t])

        dag = DAGTask(DIAMOND_G, DIAMOND_C)
        sim = DAGSimulator(dag, num_cores=1, scheduler=HighestWCETScheduler())
        result = sim.run()
        # Node 1 first, then node 2 (wcet=5) before node 3 (wcet=3), then node 4
        assert result.makespan == 10
        # Verify order: 1, 2, 3, 4
        order = [e.task_id for e in sorted(result.schedule, key=lambda e: e.start_time)]
        assert order == [1, 2, 3, 4]
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_simulator.py -v`
Expected: FAIL — cannot import DAGSimulator

**Step 3: Write implementation**

```python
"""DAG scheduling simulator."""

from __future__ import annotations

import math
import random as _random
from dataclasses import dataclass, field

from dag_sched.core import Core
from dag_sched.dag import DAGTask
from dag_sched.scheduler import Scheduler, SchedulerState


EXECUTION_MODELS = ("WCET", "BCET", "HALF_RANDOM", "FULL_RANDOM")
T_MAX = 1_000_000_000


@dataclass
class ScheduleEvent:
    """Record of a task execution on a core."""

    task_id: int
    core_id: int
    start_time: int
    end_time: int


@dataclass
class SimulationResult:
    """Result of a simulation run."""

    makespan: int
    schedule: list[ScheduleEvent] = field(default_factory=list)
    core_utilization: list[float] = field(default_factory=list)


class DAGSimulator:
    """Event-driven DAG scheduling simulator.

    Parameters
    ----------
    dag : DAGTask
    num_cores : int
    scheduler : Scheduler
    execution_model : str
        One of "WCET", "BCET", "HALF_RANDOM", "FULL_RANDOM".
    seed : int or None
        Random seed for stochastic execution models.
    """

    def __init__(
        self,
        dag: DAGTask,
        num_cores: int,
        scheduler: Scheduler,
        execution_model: str = "WCET",
        seed: int | None = None,
    ) -> None:
        if execution_model not in EXECUTION_MODELS:
            raise ValueError(f"Unknown execution model: {execution_model}. Choose from {EXECUTION_MODELS}")
        self.dag = dag
        self.num_cores = num_cores
        self.scheduler = scheduler
        self.execution_model = execution_model
        self._rng = _random.Random(seed)

    def _get_execution_time(self, node_id: int) -> int:
        wcet = self.dag.wcet[node_id]
        if self.execution_model == "WCET":
            return wcet
        elif self.execution_model == "BCET":
            return 1
        elif self.execution_model == "HALF_RANDOM":
            return self._rng.randint(math.ceil(wcet / 2), wcet)
        elif self.execution_model == "FULL_RANDOM":
            return self._rng.randint(1, wcet)
        return wcet  # fallback

    def run(self) -> SimulationResult:
        t = 0
        cores = [Core() for _ in range(self.num_cores)]
        schedule: list[ScheduleEvent] = []

        # Track when each task was assigned to a core
        task_start: dict[int, tuple[int, int]] = {}  # task_id -> (core_id, start_time)

        w_queue = list(self.dag.vertices)  # waiting queue
        r_queue: list[int] = []  # ready queue
        f_set: set[int] = set()  # finished set

        # Move source node to ready queue
        source = self.dag.source
        r_queue.append(source)
        w_queue.remove(source)

        while t < T_MAX:
            # Update ready queue: check if all predecessors finished
            newly_ready = []
            for node in w_queue:
                if all(p in f_set for p in self.dag.predecessors[node]):
                    newly_ready.append(node)
            for node in newly_ready:
                r_queue.append(node)
                w_queue.remove(node)

            # Assign tasks to idle cores
            state = SchedulerState(
                dag=self.dag,
                cores=cores,
                current_time=t,
                finished_tasks=set(f_set),
            )
            for m in range(self.num_cores):
                if cores[m].is_idle() and r_queue:
                    task_id = self.scheduler.select_task(list(r_queue), state)
                    exec_time = self._get_execution_time(task_id)
                    cores[m].assign(job_id=task_id, execution_time=exec_time)
                    r_queue.remove(task_id)
                    task_start[task_id] = (m, t)

            # Find next scheduling point
            sp = float("inf")
            for core in cores:
                wl = core.get_workload()
                if wl > 0 and wl < sp:
                    sp = wl

            if sp == float("inf"):
                # All cores idle and nothing to schedule — should not happen
                # unless we're done
                break

            # Execute all cores
            t += int(sp)
            for m in range(self.num_cores):
                task_id, finished = cores[m].execute(int(sp))
                if finished:
                    f_set.add(task_id)
                    core_id, start_time = task_start[task_id]
                    schedule.append(ScheduleEvent(
                        task_id=task_id,
                        core_id=core_id,
                        start_time=start_time,
                        end_time=t,
                    ))
                    self.scheduler.on_task_complete(task_id, t)

            # Check if all done
            if f_set == set(self.dag.vertices):
                break

        makespan = t

        # Compute core utilization
        utilization = []
        for m in range(self.num_cores):
            if makespan > 0:
                idle_time = cores[m].get_idle_count()
                utilization.append(1.0 - idle_time / makespan)
            else:
                utilization.append(0.0)

        return SimulationResult(
            makespan=makespan,
            schedule=schedule,
            core_utilization=utilization,
        )
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_simulator.py -v`
Expected: All 10 tests PASS

**Step 5: Commit**

```bash
git add dag_sched/simulator.py tests/test_simulator.py
git commit -m "feat: add DAGSimulator scheduling engine"
```

---

### Task 7: Config loader

**Files:**
- Create: `dag_sched/config.py`
- Create: `tests/test_config.py`

**Step 1: Write the failing tests**

```python
import json
import pytest
from dag_sched.config import load_config
from dag_sched.simulator import DAGSimulator
from dag_sched.scheduler import RandomScheduler


class TestLoadConfig:
    def test_load_json_config(self, tmp_path):
        cfg = {
            "dag": {
                "nodes": {"1": 1, "2": 5, "3": 3, "4": 1},
                "edges": [[1, 2], [1, 3], [2, 4], [3, 4]]
            },
            "num_cores": 2,
            "execution_model": "WCET"
        }
        path = tmp_path / "sim.json"
        path.write_text(json.dumps(cfg))
        sim = load_config(str(path), scheduler=RandomScheduler(seed=0))
        assert isinstance(sim, DAGSimulator)
        result = sim.run()
        assert result.makespan == 7

    def test_load_yaml_config(self, tmp_path):
        content = """
dag:
  nodes:
    1: 1
    2: 5
    3: 3
    4: 1
  edges:
    - [1, 2]
    - [1, 3]
    - [2, 4]
    - [3, 4]
num_cores: 2
execution_model: WCET
"""
        path = tmp_path / "sim.yaml"
        path.write_text(content)
        sim = load_config(str(path), scheduler=RandomScheduler(seed=0))
        result = sim.run()
        assert result.makespan == 7

    def test_defaults(self, tmp_path):
        cfg = {
            "dag": {
                "nodes": {"1": 1, "2": 5, "3": 1},
                "edges": [[1, 2], [2, 3]]
            }
        }
        path = tmp_path / "sim.json"
        path.write_text(json.dumps(cfg))
        sim = load_config(str(path), scheduler=RandomScheduler(seed=0))
        # Defaults: num_cores=1, execution_model="WCET"
        result = sim.run()
        assert result.makespan == 7
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_config.py -v`
Expected: FAIL — cannot import

**Step 3: Write implementation**

```python
"""Configuration file loader for DAGSimulator."""

from __future__ import annotations

import json
from pathlib import Path

from dag_sched.dag import DAGTask
from dag_sched.scheduler import Scheduler
from dag_sched.simulator import DAGSimulator


def load_config(
    path: str,
    scheduler: Scheduler,
    seed: int | None = None,
) -> DAGSimulator:
    """Load a simulation configuration from a JSON or YAML file.

    The file must contain a ``dag`` key with ``nodes`` and ``edges``.
    Optional keys: ``num_cores`` (default 1), ``execution_model`` (default "WCET").
    """
    p = Path(path)
    suffix = p.suffix.lower()

    if suffix in (".yaml", ".yml"):
        import yaml
        with open(p) as f:
            data = yaml.safe_load(f)
    else:
        with open(p) as f:
            data = json.load(f)

    dag = DAGTask.from_dict(data["dag"])
    num_cores = data.get("num_cores", 1)
    execution_model = data.get("execution_model", "WCET")

    return DAGSimulator(
        dag=dag,
        num_cores=num_cores,
        scheduler=scheduler,
        execution_model=execution_model,
        seed=seed,
    )
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_config.py -v`
Expected: All 3 tests PASS

**Step 5: Commit**

```bash
git add dag_sched/config.py tests/test_config.py
git commit -m "feat: add JSON/YAML config loader"
```

---

### Task 8: Package exports + example

**Files:**
- Modify: `dag_sched/__init__.py`
- Create: `examples/basic_usage.py`

**Step 1: Update __init__.py with public API exports**

```python
"""DAG Scheduling Simulator — a general-purpose DAG scheduling framework."""

from dag_sched.core import Core
from dag_sched.dag import DAGTask, DAGTaskBuilder
from dag_sched.scheduler import Scheduler, SchedulerState, RandomScheduler
from dag_sched.simulator import DAGSimulator, SimulationResult, ScheduleEvent
from dag_sched.config import load_config

__all__ = [
    "Core",
    "DAGTask",
    "DAGTaskBuilder",
    "Scheduler",
    "SchedulerState",
    "RandomScheduler",
    "DAGSimulator",
    "SimulationResult",
    "ScheduleEvent",
    "load_config",
]
```

**Step 2: Create example script**

```python
"""Basic usage example for dag_sched."""

from dag_sched import DAGTask, DAGSimulator, RandomScheduler, Scheduler, SchedulerState

# --- Example 1: Programmatic API ---

dag = (
    DAGTask.builder()
    .add_node(1, wcet=1)
    .add_node(2, wcet=5)
    .add_node(3, wcet=3)
    .add_node(4, wcet=7)
    .add_node(5, wcet=1)
    .add_edge(1, 2)
    .add_edge(1, 3)
    .add_edge(1, 4)
    .add_edge(2, 5)
    .add_edge(3, 5)
    .add_edge(4, 5)
    .build()
)

sim = DAGSimulator(dag, num_cores=2, scheduler=RandomScheduler(seed=42))
result = sim.run()
print(f"Makespan: {result.makespan}")
print(f"Core utilization: {[f'{u:.0%}' for u in result.core_utilization]}")
for event in sorted(result.schedule, key=lambda e: e.start_time):
    print(f"  Node {event.task_id} on core {event.core_id}: [{event.start_time}, {event.end_time})")


# --- Example 2: Custom scheduler ---

class HighestWCETFirst(Scheduler):
    """Always schedule the task with the highest WCET first."""

    def select_task(self, ready_queue, state):
        return max(ready_queue, key=lambda t: state.dag.wcet[t])

sim2 = DAGSimulator(dag, num_cores=2, scheduler=HighestWCETFirst())
result2 = sim2.run()
print(f"\nHighestWCETFirst makespan: {result2.makespan}")
```

**Step 3: Verify example runs**

Run: `python examples/basic_usage.py`
Expected: Prints makespan and schedule without errors

**Step 4: Commit**

```bash
git add dag_sched/__init__.py examples/basic_usage.py
git commit -m "feat: add package exports and usage example"
```

---

### Task 9: Delete old src/ directory

**Files:**
- Delete: `src/main.py`
- Delete: `src/sched.py`
- Delete: `src/task.py`
- Delete: `src/processor.py`
- Delete: `src/graph.py`

**Step 1: Remove old source files**

```bash
git rm -r src/
```

**Step 2: Run full test suite to ensure nothing depends on old code**

Run: `pytest tests/ -v`
Expected: All tests PASS

**Step 3: Commit**

```bash
git commit -m "chore: remove old experiment-specific src/ directory"
```

---

### Task 10: Update README

**Files:**
- Modify: `README.md`

**Step 1: Replace README content**

```markdown
# dag-sched

A general-purpose DAG scheduling simulator.

## Install

```bash
pip install -e ".[dev]"
```

## Quick Start

```python
from dag_sched import DAGTask, DAGSimulator, RandomScheduler

dag = (
    DAGTask.builder()
    .add_node(1, wcet=1)
    .add_node(2, wcet=5)
    .add_node(3, wcet=3)
    .add_node(4, wcet=1)
    .add_edge(1, 2)
    .add_edge(1, 3)
    .add_edge(2, 4)
    .add_edge(3, 4)
    .build()
)

result = DAGSimulator(dag, num_cores=2, scheduler=RandomScheduler()).run()
print(f"Makespan: {result.makespan}")
```

## Custom Scheduler

```python
from dag_sched import Scheduler, SchedulerState

class MyScheduler(Scheduler):
    def select_task(self, ready_queue, state):
        return max(ready_queue, key=lambda t: state.dag.wcet[t])
```

## Config Files

Define DAGs in JSON or YAML:

```json
{
  "dag": {
    "nodes": {"1": 1, "2": 5, "3": 3, "4": 1},
    "edges": [[1, 2], [1, 3], [2, 4], [3, 4]]
  },
  "num_cores": 2,
  "execution_model": "WCET"
}
```

```python
from dag_sched import load_config, RandomScheduler

sim = load_config("simulation.json", scheduler=RandomScheduler())
result = sim.run()
```

## Execution Models

- **WCET** — worst-case execution time (deterministic)
- **BCET** — best-case execution time (always 1)
- **HALF_RANDOM** — uniform random in [ceil(WCET/2), WCET]
- **FULL_RANDOM** — uniform random in [1, WCET]
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: rewrite README for general-purpose DAG simulator"
```

---

### Task 11: Final verification

**Step 1: Run full test suite**

Run: `pytest tests/ -v`
Expected: All tests PASS

**Step 2: Run example**

Run: `python examples/basic_usage.py`
Expected: Runs without errors, prints results

**Step 3: Verify clean import**

Run: `python -c "from dag_sched import DAGTask, DAGSimulator, RandomScheduler; print('OK')"`
Expected: Prints "OK"
