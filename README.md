# dag-sched

A general-purpose DAG scheduling simulator with pluggable scheduling algorithms.

## Installation

```bash
pip install -e .
```

## Quick Start

```python
from dag_sched import DAGTask, DAGSimulator, RandomScheduler

# Build a DAG
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

# Run simulation
sim = DAGSimulator(dag, num_cores=2, scheduler=RandomScheduler(seed=42))
result = sim.run()
print(f"Makespan: {result.makespan}")
print(f"Core utilization: {result.core_utilization}")
```

## Custom Schedulers

Subclass `Scheduler` and implement `select_task`:

```python
from dag_sched import Scheduler, SchedulerState

class HighestWCETFirst(Scheduler):
    def select_task(self, ready_queue: list[int], state: SchedulerState) -> int:
        return max(ready_queue, key=lambda t: state.dag.wcet[t])
```

## Loading from Config Files

DAGs can be loaded from JSON or YAML:

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

sim = load_config("config.json", scheduler=RandomScheduler())
result = sim.run()
```

## Execution Models

- **WCET** — worst-case execution time (deterministic)
- **BCET** — best-case, always 1 (deterministic)
- **HALF_RANDOM** — uniform random in `[ceil(WCET/2), WCET]`
- **FULL_RANDOM** — uniform random in `[1, WCET]`

## Testing

```bash
pytest tests/ -v
```
