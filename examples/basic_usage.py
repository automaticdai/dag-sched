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
