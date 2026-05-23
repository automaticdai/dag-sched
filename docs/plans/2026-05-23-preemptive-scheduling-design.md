# Preemptive Scheduling — Design

## Goal

Add preemptive scheduling to `dag-sched` as an opt-in capability, without changing the behavior of any existing non-preemptive scheduler. The simulator consults a preemptive scheduler at every event boundary for a full core→task assignment and may swap any running task for any other.

## Decisions (locked)

1. **Preemption trigger**: scheduler-driven. The scheduler returns a full `{core_id: task_id | None}` assignment each time it is consulted. No time-slice, no implicit priority comparison — the scheduler is fully in charge.
2. **Cost model**: configurable `preemption_cost: int = 0` parameter on `DAGSimulator`. Charged per swap (one swap = one core whose running task changes within a round). The total round cost is `swap_count * preemption_cost`. During the cost interval, no core makes progress (global pause). Simplification, documented as such; finer-grained cost models are out of scope for v1.
3. **Interface coexistence**: keep the existing `Scheduler.select_task(...)` for non-preemptive schedulers. Add a new abstract class `PreemptiveScheduler(Scheduler)` that defines `assign(...)`. `DAGSimulator` picks the code path via `isinstance(scheduler, PreemptiveScheduler)`. No `preemptive=True` flag on the simulator.
4. **Schedule trace**: a preempted task produces one `ScheduleEvent` per execution segment. Same `task_id` may appear multiple times with different `(core_id, start_time, end_time)` triples. Aggregating to per-task totals is a `groupby(task_id)` away.

## Public API

### `dag_sched/scheduler.py` — new class

```python
class PreemptiveScheduler(Scheduler):
    """Scheduler that produces a full core→task assignment each event boundary.

    Returned dict semantics:
      - missing key for a core → "no change" (don't preempt; don't dispatch)
      - core_id: None          → "idle this core" (preempt without replacement)
      - core_id: task_id       → "this task should run here" (dispatch, or
                                 preempt the current task if different)
    """

    @abstractmethod
    def assign(
        self,
        ready_queue: list[int],
        running: dict[int, int],
        state: SchedulerState,
    ) -> dict[int, int | None]:
        ...

    def select_task(self, ready_queue, state):
        raise NotImplementedError(
            "PreemptiveScheduler uses assign(); select_task() is not called."
        )
```

`Scheduler` (existing) is unchanged. `RandomScheduler` (existing) is unchanged.

### `dag_sched/simulator.py` — `DAGSimulator.__init__` adds one parameter

```python
def __init__(
    self,
    dag: DAGTask,
    num_cores: int,
    scheduler: Scheduler,
    execution_model: str = "WCET",
    seed: int | None = None,
    preemption_cost: int = 0,    # NEW; only valid with a PreemptiveScheduler
) -> None:
    ...
```

Construction-time validation: `preemption_cost > 0` with a non-`PreemptiveScheduler` raises `ValueError("preemption_cost is only valid with a PreemptiveScheduler")`. `preemption_cost < 0` raises `ValueError`.

### `dag_sched/core.py` — one new method

```python
def preempt(self) -> int:
    """Stop the current job, return its remaining workload, become idle.
    Raises RuntimeError if called on an idle core."""
```

No other Core change. `_idle_count` keeps its existing meaning.

### `dag_sched/__init__.py` — export `PreemptiveScheduler`

### `ScheduleEvent` — shape unchanged

`ScheduleEvent(task_id, core_id, start_time, end_time)` keeps its current fields. Docstring updated to note that preempted tasks produce multiple segments.

## Event loop (preemptive path)

The non-preemptive path is left byte-identical. The preemptive path runs when `isinstance(scheduler, PreemptiveScheduler)` is True.

```
remaining_workload: dict[task_id, int]   # populated at first dispatch
task_start: dict[task_id, (core_id, start_time)]  # for the currently-running segment

while not all tasks finished:
    1. newly-ready = nodes whose predecessors are all in f_set; move them into ready_queue.
    2. running = {core_id: core.get_running_task() for non-idle cores}
    3. state   = SchedulerState(dag, cores, current_time=t, finished_tasks=copy(f_set))
    4. assignment = scheduler.assign(ready_queue, running, state)
    5. Validate assignment (see "Errors" below).

    6. Pass 1 — preempt every core whose running task is changing.
       swap_cores = []
       for c in range(num_cores):
           if c not in assignment: continue                # "no change"
           desired = assignment[c]
           current = running.get(c)
           if desired == current: continue                 # no-op

           if current is not None:
               remaining = core[c].preempt()
               remaining_workload[current] = remaining
               ready_queue.append(current)
               emit ScheduleEvent(current, *task_start[current], end_time=t)
           swap_cores.append((c, desired))

    7. Advance global clock through the cost interval (no work done by anyone),
       but only if at least one currently-running task was actually displaced.
       Idle-to-task dispatches by themselves do not incur cost.
       preempted_count = sum(1 for c, _ in swap_cores if running.get(c) is not None)
       if preempted_count > 0:
           cost_interval = len(swap_cores) * preemption_cost
           t += cost_interval
           for c in cores: c.add_idle_time(cost_interval)  # so utilization stays accurate

    8. Pass 2 — dispatch new tasks at the post-cost t.
       for (c, desired) in swap_cores:
           if desired is None: continue                    # core stays idle
           if desired not in remaining_workload:
               remaining_workload[desired] = _get_execution_time(desired)
           core[c].assign(desired, remaining_workload[desired])
           ready_queue.remove(desired)
           task_start[desired] = (c, t)                    # new segment starts after cost

    9. sp = min positive workload across cores; same stall-detection as today.
    10. t += sp; for each core: (task_id, finished) = core.execute(sp)
        if finished:
            emit ScheduleEvent(task_id, *task_start[task_id], end_time=t)
            f_set.add(task_id); scheduler.on_task_complete(task_id, t)
            remaining_workload.pop(task_id, None)
```

### Notes on semantics

- **"No change" vs "idle"**: missing key = leave alone. Explicit `None` = preempt without replacement. A scheduler that returns `{}` is a no-op; a scheduler that wants to halt every core returns `{c: None for c in range(num_cores)}`.
- **Stochastic execution time, sampled once**: `_get_execution_time(task_id)` is called once on first dispatch and the result is stored in `remaining_workload`. Subsequent preemption/resume cycles use the stored remaining time. Re-sampling on resume would be physically nonsensical (a task's true runtime is a property of the job, not of the scheduler).
- **Preemption cost charged per swap, with global pause, gated on actual displacement**: cost is charged only in rounds where at least one currently-running task is preempted (displaced from its core). In such rounds, `t` advances by `N * preemption_cost` where N is the count of all core changes in that round (preemptions AND any accompanying idle-to-task dispatches). During this interval, no core makes progress — coarse-grained "everything pauses for the OS" model, and all cores accumulate idle time so utilization stays accurate. A core that returns to the same task or is left alone (key missing from `assign`'s result) contributes 0. A round with only idle-to-task dispatches (no preemption) charges no cost.
- **Segment recording**: a new segment is opened on dispatch and closed on either natural completion or preemption. Same `ScheduleEvent` shape in both cases.

## Errors

All validation raises `ValueError` at the point of failure with a message naming the offending field. Failing fast catches scheduler bugs that would otherwise manifest as silent stalls.

1. `preemption_cost > 0` with a non-preemptive scheduler → ValueError at construction.
2. `preemption_cost < 0` → ValueError at construction.
3. `assign()` returns a `core_id` outside `[0, num_cores)` → ValueError naming the bad key.
4. `assign()` returns a `task_id` that is neither in `ready_queue` nor currently running → ValueError naming the bad task.
5. `assign()` returns the same `task_id` on two different cores → ValueError naming the task.
6. `Core.preempt()` called on idle core → RuntimeError (defensive; simulator never calls it on idle).
7. Existing stall detection (`sp == inf` with unfinished tasks) is unchanged and serves as a final safety net.

## Testing

New file: `tests/test_preemption.py`.

- `test_preemptive_scheduler_can_swap_two_tasks` — handcrafted DAG with a scheduler that swaps task A for task B at a known time on a known core; assert two segments exist for A in the schedule.
- `test_preempted_task_resumes_with_remaining_workload` — task with WCET=10, preempted after 3, must run 7 more units. Assert segment durations and total makespan.
- `test_preemption_cost_extends_makespan` — same DAG, `preemption_cost=0` vs `preemption_cost=5`; assert makespan delta equals 5 × (number of swaps).
- `test_non_preemptive_scheduler_path_unchanged` — `RandomScheduler` with fixed seed; assert makespan and schedule match a saved baseline. Regression guard for the existing path.
- `test_assign_returns_unknown_task_raises` — validation.
- `test_assign_returns_same_task_on_two_cores_raises` — validation.
- `test_assign_returns_invalid_core_id_raises` — validation.
- `test_preemption_cost_requires_preemptive_scheduler` — construction-time validation.
- `test_preemption_cost_negative_raises` — construction-time validation.
- `test_stochastic_execution_time_not_resampled_on_resume` — `FULL_RANDOM` execution model, fixed seed, preempt mid-execution; assert total executed time equals the first-sampled value.
- `test_assignment_missing_key_means_no_change` — scheduler returns `{}`; assert running tasks keep running.
- `test_assignment_none_value_means_idle_core` — scheduler returns `{0: None}`; assert task 0 is preempted and core 0 is idle until next round.

## Docs

- `README.md` — new "Preemptive scheduling" section after "Custom Schedulers", with a minimal `PreemptiveScheduler` example (~15 lines).
- `dag_sched/__init__.py` — export `PreemptiveScheduler`.

## Out of scope (v1 non-goals)

- Per-task non-preemptible flag.
- FPDS-style preemption threshold.
- Separate migration cost (preempted-then-resumed-on-different-core costs the same as resumed-on-same-core).
- Built-in preemptive scheduler beyond what test fixtures need; users supply their own. Matches the project's "bring-your-own-scheduler" stance.
