# General DAG Scheduling Simulator — Design

## Goal

Convert the experiment-specific DAG scheduling codebase into a general-purpose DAG scheduling simulator with clean APIs for defining DAGs, plugging in custom schedulers, and running simulations.

## Package Structure

```
dag-sched-rl/
├── pyproject.toml
├── README.md
├── dag_sched/
│   ├── __init__.py          # Public API exports
│   ├── dag.py               # DAGTask data structure + factory methods
│   ├── job.py               # Job (runtime instance of a DAG node)
│   ├── core.py              # Core (processing unit)
│   ├── scheduler.py         # Scheduler ABC + built-in RandomScheduler
│   ├── simulator.py         # DAGSimulator - the main scheduling loop
│   ├── graph.py             # Graph utility functions
│   └── config.py            # JSON/YAML config loader
├── examples/
│   └── basic_usage.py       # Example showing API + config usage
└── tests/
    └── test_simulator.py    # Basic tests
```

The old `src/` directory is deleted entirely.

## Core Components

### DAGTask (`dag.py`)

Represents a Directed Acyclic Graph task with execution time metadata.

**Fields:**
- `G` — adjacency dict `{node_id: [successor_ids]}`
- `C` — dict `{node_id: wcet}`
- `V` — sorted list of all vertex IDs
- `pre` — dict `{node_id: [predecessor_ids]}`

**Factory methods:**
- `DAGTask.from_dict(data)` — build from a Python dict
- `DAGTask.from_json(path)` — load from JSON file
- `DAGTask.from_yaml(path)` — load from YAML file

**Programmatic builder:**
- `dag = DAGTask()`
- `dag.add_node(id, wcet)`
- `dag.add_edge(src, dst)`
- `dag.validate()` — checks: acyclic, single source, single sink, no orphans

### Job (`job.py`)

Runtime execution instance of a DAG node.

**Fields:**
- `idx` — node ID
- `of_task` — reference to parent DAGTask
- `C` — actual execution time (may differ from WCET depending on execution model)
- `wcet` — worst-case execution time

### Core (`core.py`)

A processing unit that executes jobs.

**Fields:**
- `idle` — whether available for assignment
- `workload` — remaining execution time
- `job_id` — currently executing job ID
- `idle_count` — accumulated idle time

**Methods:**
- `assign(job)` — load a job onto this core
- `execute(t)` — run current job for t time units, return (job_id, finished)
- `is_idle()` — check availability
- `get_workload()` — remaining time

### Scheduler (`scheduler.py`)

Abstract base class for scheduling algorithms.

```python
class Scheduler(ABC):
    @abstractmethod
    def select_task(self, ready_queue: list[int], state: SchedulerState) -> int:
        """Pick a task ID from the ready queue to schedule next."""
        ...

    def on_task_complete(self, task_id: int, time: int) -> None:
        """Optional hook when a task finishes."""
        pass

class SchedulerState:
    """Read-only view of simulator state."""
    dag: DAGTask
    cores: list[Core]
    current_time: int
    finished_tasks: set[int]
```

**Built-in:** `RandomScheduler` — picks randomly from ready queue.

### DAGSimulator (`simulator.py`)

Main simulation engine.

```python
class DAGSimulator:
    def __init__(self, dag, num_cores, scheduler, execution_model="WCET", seed=None)
    def run(self) -> SimulationResult

class SimulationResult:
    makespan: int
    schedule: list[ScheduleEvent]  # (task_id, core_id, start_time, end_time)
    core_utilization: list[float]
```

**Scheduling loop** (event-driven):
1. Update ready queue (check predecessor completion)
2. For each idle core: ask scheduler to select task
3. Find next scheduling point (minimum remaining workload)
4. Execute all cores for that interval
5. Mark finished tasks, notify scheduler
6. Repeat until all nodes complete

### Graph Utilities (`graph.py`)

Cleaned-up versions of existing functions:
- `find_all_paths(G, start, end)`
- `find_longest_path(G, start, end, weights)`
- `find_predecessors(G, node)`
- `find_successors(G, node)`
- `find_ancestors(G, node)`
- `find_descendants(G, node)`

No mutable default arguments. No hardcoded source/sink assumptions.

### Config Loader (`config.py`)

Loads JSON/YAML configuration files describing a simulation:

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

Returns a configured `DAGSimulator` (scheduler must be provided separately since it's a class).

## Execution Models

- **WCET** — deterministic, worst-case execution time
- **BCET** — deterministic, best-case (set to 1 if not specified)
- **HALF_RANDOM** — uniform random in `[ceil(WCET/2), WCET]`
- **FULL_RANDOM** — uniform random in `[1, WCET]`

## What Gets Removed

- `Simulator` class (experiment sweep orchestration in main.py)
- All `rta_alphabeta_new` references
- `eligibility`, `TPDS2019`, `EMSOFT2019` algorithms (depend on missing module)
- `EO_v1()` function
- `Cache`, `Storage`, `Processor` stubs
- `.gpickle` loading
- CSV-style output formatting
- `HALF_RANDOM_NORM`, `FULL_RANDOM_NORM` (unused)

## Decisions

- **DAG input:** Both programmatic API and JSON/YAML config files
- **Scheduler interface:** Strategy class (ABC) pattern
- **Execution models:** Keep stochastic models (WCET, BCET, HALF_RANDOM, FULL_RANDOM)
- **Approach:** Full rewrite into clean package structure
