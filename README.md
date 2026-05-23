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

## Preemptive Scheduling

For algorithms that need to interrupt and reassign running tasks, subclass `PreemptiveScheduler` and implement `assign`. The simulator consults it at every event boundary and applies the full core→task assignment, preempting tasks whose assignment has changed.

```python
from dag_sched import DAGTask, DAGSimulator, PreemptiveScheduler

class GreedyPreemptive(PreemptiveScheduler):
    def assign(self, ready_queue, running, state):
        assignment = {}
        ready = list(ready_queue)
        for c in range(len(state.cores)):
            if c not in running and ready:
                assignment[c] = ready.pop(0)
        return assignment

dag = DAGTask.builder().add_node(1, wcet=1).add_node(2, wcet=1).add_edge(1, 2).build()
sim = DAGSimulator(dag, num_cores=2, scheduler=GreedyPreemptive(), preemption_cost=0)
result = sim.run()
```

- Returning `{core_id: task_id}` dispatches a task; `{core_id: None}` idles the core; omitting a `core_id` leaves it alone.
- A preempted task is returned to the ready queue with its remaining workload and resumes from there.
- A preempted task produces one `ScheduleEvent` per execution segment (same `task_id`, different `(core_id, start_time, end_time)`).
- `preemption_cost` adds `swap_count * preemption_cost` time units in any round that involves at least one preemption (a running task being displaced); idle-to-task dispatches do not incur cost.

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
