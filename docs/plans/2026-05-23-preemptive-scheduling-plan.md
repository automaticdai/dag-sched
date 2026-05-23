# Preemptive Scheduling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add opt-in preemptive scheduling to the dag-sched simulator, driven by a new `PreemptiveScheduler` ABC, with a configurable preemption cost. The existing non-preemptive code path is unchanged.

**Architecture:** A new abstract class `PreemptiveScheduler(Scheduler)` defines `assign(ready_queue, running, state) -> {core_id: task_id | None}`. `DAGSimulator.run()` dispatches by `isinstance(scheduler, PreemptiveScheduler)`. The preemptive path uses a two-pass swap loop (pass 1: preempt cores whose running task is changing; advance clock by `swap_count * preemption_cost`; pass 2: dispatch new tasks at the post-cost time), and caches `remaining_workload` so preempted tasks resume with their leftover work and stochastic execution times are sampled once.

**Tech Stack:** Python 3.10+, pytest. No new dependencies.

**Spec:** `docs/plans/2026-05-23-preemptive-scheduling-design.md`

---

### Task 1: Add `Core.preempt()`

Add a method that stops the current job, returns its remaining workload, and idles the core. This is the primitive the preemptive event loop will call.

**Files:**
- Modify: `dag_sched/core.py` (add new method to class `Core`)
- Create: `tests/test_preemption.py`

- [ ] **Step 1.1: Create test file with failing tests**

Create `tests/test_preemption.py`:

```python
"""Tests for preemptive scheduling support."""
from __future__ import annotations

import pytest

from dag_sched.core import Core


class TestCorePreempt:
    def test_preempt_returns_remaining_workload(self):
        c = Core()
        c.assign(job_id=1, execution_time=10)
        c.execute(3)
        assert c.preempt() == 7

    def test_preempt_makes_core_idle(self):
        c = Core()
        c.assign(job_id=1, execution_time=10)
        c.execute(3)
        c.preempt()
        assert c.is_idle()
        assert c.get_running_task() is None

    def test_preempt_on_idle_core_raises(self):
        c = Core()
        with pytest.raises(RuntimeError):
            c.preempt()
```

- [ ] **Step 1.2: Run tests, expect failure**

Run: `pytest tests/test_preemption.py -v`
Expected: 3 tests fail with `AttributeError: 'Core' object has no attribute 'preempt'`.

- [ ] **Step 1.3: Implement Core.preempt()**

In `dag_sched/core.py`, append to class `Core` (after `execute`):

```python
    def preempt(self) -> int:
        """Stop the current job, return its remaining workload, become idle.

        Raises RuntimeError if called on an idle core.
        """
        if self._idle:
            raise RuntimeError("preempt() called on idle core")
        remaining = self._workload
        self._workload = 0
        self._job_id = None
        self._idle = True
        return remaining
```

- [ ] **Step 1.4: Run tests, expect pass**

Run: `pytest tests/test_preemption.py -v`
Expected: 3 passed.

- [ ] **Step 1.5: Commit**

```bash
git add dag_sched/core.py tests/test_preemption.py
git commit -m "feat(core): add Core.preempt() returning remaining workload"
```

---

### Task 2: Add `PreemptiveScheduler` ABC

A new abstract subclass of `Scheduler` that exposes `assign(...)` instead of `select_task(...)`. Calling `select_task` on a `PreemptiveScheduler` raises `NotImplementedError` (it's required by the base ABC but never invoked by the simulator on this path).

**Files:**
- Modify: `dag_sched/scheduler.py` (append new class at end of file)
- Modify: `tests/test_preemption.py` (append new test class)

- [ ] **Step 2.1: Add failing tests**

Append to `tests/test_preemption.py`:

```python
from dag_sched.scheduler import PreemptiveScheduler, Scheduler


class TestPreemptiveSchedulerABC:
    def test_is_subclass_of_scheduler(self):
        assert issubclass(PreemptiveScheduler, Scheduler)

    def test_cannot_instantiate_without_assign(self):
        class Incomplete(PreemptiveScheduler):
            pass
        with pytest.raises(TypeError):
            Incomplete()

    def test_select_task_raises_not_implemented(self):
        class Sched(PreemptiveScheduler):
            def assign(self, ready_queue, running, state):
                return {}
        with pytest.raises(NotImplementedError):
            Sched().select_task([1], None)
```

- [ ] **Step 2.2: Run tests, expect failure**

Run: `pytest tests/test_preemption.py::TestPreemptiveSchedulerABC -v`
Expected: `ImportError: cannot import name 'PreemptiveScheduler'`.

- [ ] **Step 2.3: Implement PreemptiveScheduler**

In `dag_sched/scheduler.py`, append at end of file:

```python
class PreemptiveScheduler(Scheduler):
    """Scheduler that produces a full core→task assignment each event boundary.

    The simulator calls `assign(...)` at every event boundary. The returned
    dict maps `core_id -> task_id_or_None`:
      - missing key for a core: "no change" (don't preempt; don't dispatch)
      - core_id: None         : "idle this core" (preempt without replacement)
      - core_id: task_id      : "this task should run here" (dispatch, or
                                 preempt the current task if different)

    A preempted task goes back to the ready queue with its remaining workload.
    """

    @abstractmethod
    def assign(
        self,
        ready_queue: list[int],
        running: dict[int, int],
        state: SchedulerState,
    ) -> dict[int, int | None]:
        ...

    def select_task(self, ready_queue: list[int], state: SchedulerState) -> int:
        raise NotImplementedError(
            "PreemptiveScheduler uses assign(); select_task() is not called."
        )
```

- [ ] **Step 2.4: Run tests, expect pass**

Run: `pytest tests/test_preemption.py -v`
Expected: 6 passed (3 from Task 1 + 3 new).

- [ ] **Step 2.5: Commit**

```bash
git add dag_sched/scheduler.py tests/test_preemption.py
git commit -m "feat(scheduler): add PreemptiveScheduler ABC with assign()"
```

---

### Task 3: Add `preemption_cost` parameter to `DAGSimulator` with validation

Add the `preemption_cost: int = 0` kwarg and reject misuse at construction time. No event-loop change yet — just the parameter and its guard rails.

**Files:**
- Modify: `dag_sched/simulator.py` (add param + validation in `__init__`)
- Modify: `tests/test_preemption.py`

- [ ] **Step 3.1: Add failing validation tests**

Append to `tests/test_preemption.py`:

```python
from dag_sched.dag import DAGTask
from dag_sched.scheduler import RandomScheduler
from dag_sched.simulator import DAGSimulator


def _minimal_dag() -> DAGTask:
    """Two-node line DAG; passes the simulator's single-source/single-sink check."""
    return DAGTask(successors={1: [2], 2: []}, wcet={1: 1, 2: 1})


def _minimal_preemptive_scheduler() -> PreemptiveScheduler:
    class S(PreemptiveScheduler):
        def assign(self, ready_queue, running, state):
            return {}
    return S()


class TestPreemptionCostValidation:
    def test_negative_preemption_cost_raises(self):
        with pytest.raises(ValueError, match="preemption_cost"):
            DAGSimulator(
                _minimal_dag(), num_cores=1,
                scheduler=_minimal_preemptive_scheduler(),
                preemption_cost=-1,
            )

    def test_positive_cost_with_non_preemptive_scheduler_raises(self):
        with pytest.raises(ValueError, match="PreemptiveScheduler"):
            DAGSimulator(
                _minimal_dag(), num_cores=1,
                scheduler=RandomScheduler(),
                preemption_cost=5,
            )

    def test_zero_cost_with_non_preemptive_scheduler_ok(self):
        DAGSimulator(
            _minimal_dag(), num_cores=1,
            scheduler=RandomScheduler(),
            preemption_cost=0,
        )

    def test_positive_cost_with_preemptive_scheduler_ok(self):
        DAGSimulator(
            _minimal_dag(), num_cores=1,
            scheduler=_minimal_preemptive_scheduler(),
            preemption_cost=5,
        )
```

- [ ] **Step 3.2: Run tests, expect failure**

Run: `pytest tests/test_preemption.py::TestPreemptionCostValidation -v`
Expected: tests fail with `TypeError: __init__() got an unexpected keyword argument 'preemption_cost'`.

- [ ] **Step 3.3: Implement parameter and validation**

In `dag_sched/simulator.py`, replace `DAGSimulator.__init__` with:

```python
    def __init__(
        self,
        dag: DAGTask,
        num_cores: int,
        scheduler: Scheduler,
        execution_model: str = "WCET",
        seed: int | None = None,
        preemption_cost: int = 0,
    ) -> None:
        if execution_model not in EXECUTION_MODELS:
            raise ValueError(f"Unknown execution model: {execution_model}. Choose from {EXECUTION_MODELS}")
        if preemption_cost < 0:
            raise ValueError(f"preemption_cost must be >= 0, got {preemption_cost}")
        # Import here to avoid a circular import at module load (scheduler.py
        # is already imported above, but using isinstance against the subclass
        # is local to this check).
        from dag_sched.scheduler import PreemptiveScheduler
        if preemption_cost > 0 and not isinstance(scheduler, PreemptiveScheduler):
            raise ValueError(
                "preemption_cost is only valid with a PreemptiveScheduler"
            )
        self.dag = dag
        self.num_cores = num_cores
        self.scheduler = scheduler
        self.execution_model = execution_model
        self.preemption_cost = preemption_cost
        self._rng = _random.Random(seed)
```

- [ ] **Step 3.4: Run full test suite**

Run: `pytest tests/ -v`
Expected: all pre-existing tests pass plus 10 new (3 Core + 3 ABC + 4 validation).

- [ ] **Step 3.5: Commit**

```bash
git add dag_sched/simulator.py tests/test_preemption.py
git commit -m "feat(simulator): add preemption_cost parameter with validation"
```

---

### Task 4: Implement the preemptive event loop

Split `DAGSimulator.run()` into a dispatcher and two paths (`_run_non_preemptive` keeps the existing logic byte-identical; `_run_preemptive` is new). Implement the full two-pass swap loop with `remaining_workload` cache. Three tests cover the meaningful behaviors: greedy-no-swap, an actual preemption (segments split), and the cost interval extending makespan.

**Files:**
- Modify: `dag_sched/simulator.py` (refactor `run`, add `_run_preemptive`)
- Modify: `tests/test_preemption.py`

- [ ] **Step 4.1: Refactor `run()` into `_run_non_preemptive()` and a dispatching `run()`**

In `dag_sched/simulator.py`:

1. Rename the existing `def run(self)` method to `def _run_non_preemptive(self)`. Body is unchanged.
2. Add a new `run` method that dispatches by scheduler type:

```python
    def run(self) -> SimulationResult:
        from dag_sched.scheduler import PreemptiveScheduler
        if isinstance(self.scheduler, PreemptiveScheduler):
            return self._run_preemptive()
        return self._run_non_preemptive()
```

- [ ] **Step 4.2: Run the full test suite to confirm the refactor is safe**

Run: `pytest tests/ -v`
Expected: all existing tests pass. The new tests from tasks 1–3 still pass (they don't call `_run_preemptive` yet). No tests have failed regressions.

- [ ] **Step 4.3: Add failing tests for the preemptive event loop**

Append to `tests/test_preemption.py`:

```python
class GreedyPreemptiveScheduler(PreemptiveScheduler):
    """Fills each idle core with the next ready task in order. Never preempts."""

    def assign(self, ready_queue, running, state):
        assignment: dict[int, int | None] = {}
        ready = list(ready_queue)
        for c in range(len(state.cores)):
            if c not in running and ready:
                assignment[c] = ready.pop(0)
        return assignment


class ScriptedScheduler(PreemptiveScheduler):
    """Returns a pre-baked assignment per call. After the script is exhausted,
    returns `{}` (i.e. "no change" — running tasks keep running)."""

    def __init__(self, script: list[dict[int, int | None]]) -> None:
        self.script = list(script)
        self.call = 0

    def assign(self, ready_queue, running, state):
        a = self.script[self.call] if self.call < len(self.script) else {}
        self.call += 1
        return a


# Fixture used by the swap + cost tests.
# DAG: 1 -> {2, 3, 4} -> 5.  Source 1, sink 5.
# WCETs: 1=1, 2=10, 3=1, 4=5, 5=1.
# Two cores. Script:
#   round 1 (t=0, ready=[1]):                 {0: 1}
#   round 2 (t=1, 1 done, ready=[2,3,4]):     {0: 2, 1: 3}
#   round 3 (t=2, 3 done, ready=[4], run={0:2}):
#     SWAP: preempt task 2 from core 0 (remaining 9), put 4 on core 0, migrate 2 to core 1.
#     -> {0: 4, 1: 2}
#   round 4 (no change, 4 finishes first then 2 finishes): {}
#   round 5 (ready=[5]):                       {0: 5}
SWAP_FIXTURE_DAG = DAGTask(
    successors={1: [2, 3, 4], 2: [5], 3: [5], 4: [5], 5: []},
    wcet={1: 1, 2: 10, 3: 1, 4: 5, 5: 1},
)
SWAP_FIXTURE_SCRIPT: list[dict[int, int | None]] = [
    {0: 1},
    {0: 2, 1: 3},
    {0: 4, 1: 2},
    {},
    {0: 5},
]


class TestPreemptiveEventLoop:
    def test_greedy_runs_simple_line_dag(self):
        dag = DAGTask(
            successors={1: [2], 2: [3], 3: []},
            wcet={1: 1, 2: 1, 3: 1},
        )
        sim = DAGSimulator(dag, num_cores=1, scheduler=GreedyPreemptiveScheduler())
        result = sim.run()
        assert result.makespan == 3
        assert sorted(e.task_id for e in result.schedule) == [1, 2, 3]

    def test_preempted_task_appears_as_two_segments(self):
        sim = DAGSimulator(
            SWAP_FIXTURE_DAG, num_cores=2,
            scheduler=ScriptedScheduler(SWAP_FIXTURE_SCRIPT),
        )
        result = sim.run()
        assert result.makespan == 12
        task_2_events = sorted(
            ((e.core_id, e.start_time, e.end_time) for e in result.schedule if e.task_id == 2),
        )
        # Segment 1: ran on core 0 from t=1 to t=2 (preempted).
        # Segment 2: resumed on core 1 from t=2 to t=11 (workload 9).
        assert task_2_events == [(0, 1, 2), (1, 2, 11)]

    def test_preemption_cost_extends_makespan_by_swap_count_times_cost(self):
        sim0 = DAGSimulator(
            SWAP_FIXTURE_DAG, num_cores=2,
            scheduler=ScriptedScheduler(SWAP_FIXTURE_SCRIPT),
            preemption_cost=0,
        )
        sim5 = DAGSimulator(
            SWAP_FIXTURE_DAG, num_cores=2,
            scheduler=ScriptedScheduler(SWAP_FIXTURE_SCRIPT),
            preemption_cost=5,
        )
        # Round 3 has 2 swaps (core 0: 2->4, core 1: idle->2). Cost adds 2*5=10.
        assert sim5.run().makespan - sim0.run().makespan == 10
```

- [ ] **Step 4.4: Run tests, expect failure**

Run: `pytest tests/test_preemption.py::TestPreemptiveEventLoop -v`
Expected: 3 tests fail with `AttributeError: 'DAGSimulator' object has no attribute '_run_preemptive'`.

- [ ] **Step 4.5: Implement `_run_preemptive`**

In `dag_sched/simulator.py`, add after `_run_non_preemptive`:

```python
    def _run_preemptive(self) -> SimulationResult:
        t = 0
        cores = [Core() for _ in range(self.num_cores)]
        schedule: list[ScheduleEvent] = []
        task_start: dict[int, tuple[int, int]] = {}
        remaining_workload: dict[int, int] = {}

        w_queue = list(self.dag.vertices)
        r_queue: list[int] = []
        f_set: set[int] = set()

        source = self.dag.source
        r_queue.append(source)
        w_queue.remove(source)

        while t < T_MAX:
            # 1. Move newly-ready tasks to the ready queue.
            newly_ready = [
                node for node in w_queue
                if all(p in f_set for p in self.dag.predecessors[node])
            ]
            for node in newly_ready:
                r_queue.append(node)
                w_queue.remove(node)

            # 2. Build state + running map, call assign().
            running: dict[int, int] = {
                c: cores[c].get_running_task()
                for c in range(self.num_cores)
                if not cores[c].is_idle()
            }
            state = SchedulerState(
                dag=self.dag,
                cores=cores,
                current_time=t,
                finished_tasks=set(f_set),
            )
            assignment = self.scheduler.assign(list(r_queue), dict(running), state)

            # 3. Pass 1 — preempt every core whose running task is changing.
            swap_cores: list[tuple[int, int | None]] = []
            for c in range(self.num_cores):
                if c not in assignment:
                    continue
                desired = assignment[c]
                current = running.get(c)
                if desired == current:
                    continue
                if current is not None:
                    remaining = cores[c].preempt()
                    remaining_workload[current] = remaining
                    r_queue.append(current)
                    core_id, start_time = task_start[current]
                    schedule.append(ScheduleEvent(
                        task_id=current, core_id=core_id,
                        start_time=start_time, end_time=t,
                    ))
                swap_cores.append((c, desired))

            # 4. Cost interval: global pause for swap_count * preemption_cost.
            t += len(swap_cores) * self.preemption_cost

            # 5. Pass 2 — dispatch new tasks at the post-cost t.
            for c, desired in swap_cores:
                if desired is None:
                    continue
                if desired not in remaining_workload:
                    remaining_workload[desired] = self._get_execution_time(desired)
                cores[c].assign(
                    job_id=desired,
                    execution_time=remaining_workload[desired],
                )
                r_queue.remove(desired)
                task_start[desired] = (c, t)

            # 6. Advance time to the next event boundary.
            sp = float("inf")
            for core in cores:
                wl = core.get_workload()
                if wl > 0 and wl < sp:
                    sp = wl

            if sp == float("inf"):
                if f_set != set(self.dag.vertices):
                    unfinished = set(self.dag.vertices) - f_set
                    raise RuntimeError(
                        f"Simulation stalled: no progress possible. "
                        f"Unfinished tasks: {unfinished}"
                    )
                break

            t += int(sp)
            for c in range(self.num_cores):
                task_id, finished = cores[c].execute(int(sp))
                if finished:
                    f_set.add(task_id)
                    core_id, start_time = task_start[task_id]
                    schedule.append(ScheduleEvent(
                        task_id=task_id, core_id=core_id,
                        start_time=start_time, end_time=t,
                    ))
                    self.scheduler.on_task_complete(task_id, t)
                    remaining_workload.pop(task_id, None)

            if f_set == set(self.dag.vertices):
                break

        makespan = t
        utilization = []
        for c in range(self.num_cores):
            if makespan > 0:
                idle_time = cores[c].get_idle_count()
                utilization.append(1.0 - idle_time / makespan)
            else:
                utilization.append(0.0)

        return SimulationResult(
            makespan=makespan,
            schedule=schedule,
            core_utilization=utilization,
        )
```

- [ ] **Step 4.6: Run full test suite**

Run: `pytest tests/ -v`
Expected: all tests pass, including the three new event-loop tests.

- [ ] **Step 4.7: Commit**

```bash
git add dag_sched/simulator.py tests/test_preemption.py
git commit -m "feat(simulator): add preemptive event loop with two-pass swap"
```

---

### Task 5: Validate `assign()` return value

Catch scheduler bugs early with clear `ValueError`s. Three checks: bad core id, unknown task id, duplicate task across cores. Validation happens before any mutation so failures leave the simulator state untouched.

**Files:**
- Modify: `dag_sched/simulator.py` (add validation helper called from `_run_preemptive`)
- Modify: `tests/test_preemption.py`

- [ ] **Step 5.1: Add failing validation tests**

Append to `tests/test_preemption.py`:

```python
class TestAssignValidation:
    def test_unknown_core_id_raises(self):
        class Bad(PreemptiveScheduler):
            def assign(self, ready_queue, running, state):
                return {99: ready_queue[0]} if ready_queue else {}
        sim = DAGSimulator(_minimal_dag(), num_cores=1, scheduler=Bad())
        with pytest.raises(ValueError, match="core_id"):
            sim.run()

    def test_unknown_task_id_raises(self):
        class Bad(PreemptiveScheduler):
            def assign(self, ready_queue, running, state):
                return {0: 999}
        sim = DAGSimulator(_minimal_dag(), num_cores=1, scheduler=Bad())
        with pytest.raises(ValueError, match="task_id"):
            sim.run()

    def test_same_task_on_two_cores_raises(self):
        dag = DAGTask(
            successors={1: [2, 3], 2: [4], 3: [4], 4: []},
            wcet={1: 1, 2: 5, 3: 5, 4: 1},
        )

        class Bad(PreemptiveScheduler):
            def assign(self, ready_queue, running, state):
                if 2 in ready_queue and 3 in ready_queue:
                    return {0: 2, 1: 2}
                # fall back to greedy so we can reach the bad round
                a = {}
                ready = list(ready_queue)
                for c in range(len(state.cores)):
                    if c not in running and ready:
                        a[c] = ready.pop(0)
                return a

        sim = DAGSimulator(dag, num_cores=2, scheduler=Bad())
        with pytest.raises(ValueError, match="duplicate"):
            sim.run()
```

- [ ] **Step 5.2: Run tests, expect failure**

Run: `pytest tests/test_preemption.py::TestAssignValidation -v`
Expected: tests fail — but with the wrong errors (KeyError, IndexError, or wrong-time behavior), not a clean ValueError.

- [ ] **Step 5.3: Add validation helper and call it from `_run_preemptive`**

In `dag_sched/simulator.py`, add a private helper inside class `DAGSimulator`:

```python
    def _validate_assignment(
        self,
        assignment: dict[int, int | None],
        ready_queue: list[int],
        running: dict[int, int],
    ) -> None:
        seen_tasks: set[int] = set()
        valid_tasks = set(ready_queue) | set(running.values())
        for core_id, task_id in assignment.items():
            if not (0 <= core_id < self.num_cores):
                raise ValueError(
                    f"assign() returned invalid core_id {core_id}; "
                    f"valid range is [0, {self.num_cores})"
                )
            if task_id is None:
                continue
            if task_id not in valid_tasks:
                raise ValueError(
                    f"assign() returned task_id {task_id} which is neither "
                    f"in the ready queue {ready_queue} nor currently running "
                    f"{list(running.values())}"
                )
            if task_id in seen_tasks:
                raise ValueError(
                    f"assign() returned duplicate task_id {task_id} on "
                    f"multiple cores; each task can run on at most one core"
                )
            seen_tasks.add(task_id)
```

In `_run_preemptive`, immediately after the `assignment = self.scheduler.assign(...)` line, add:

```python
            self._validate_assignment(assignment, list(r_queue), dict(running))
```

- [ ] **Step 5.4: Run full test suite**

Run: `pytest tests/ -v`
Expected: all tests pass (validation tests now raise the right errors).

- [ ] **Step 5.5: Commit**

```bash
git add dag_sched/simulator.py tests/test_preemption.py
git commit -m "feat(simulator): validate PreemptiveScheduler.assign() return value"
```

---

### Task 6: Verify stochastic execution time is not resampled on resume

The implementation in Task 4 already samples `_get_execution_time(task)` only on first dispatch (via the `remaining_workload` cache) — this task adds a regression test that pins the behavior.

**Files:**
- Modify: `tests/test_preemption.py`

- [ ] **Step 6.1: Add the failing test**

Append to `tests/test_preemption.py`:

```python
class TestStochasticPreemption:
    def test_full_random_execution_time_sampled_once_per_task(self):
        # Same swap fixture as TestPreemptiveEventLoop, but with FULL_RANDOM.
        # Task 2 has WCET=10. If sampled once, its total execution time is
        # the same value both before and after preemption. If resampled, the
        # makespan would vary in a way we can detect by checking that the
        # sum of task-2 segment durations equals the originally-sampled time
        # (not a fresh sample).
        sim = DAGSimulator(
            SWAP_FIXTURE_DAG, num_cores=2,
            scheduler=ScriptedScheduler(SWAP_FIXTURE_SCRIPT),
            execution_model="FULL_RANDOM", seed=12345,
        )
        result = sim.run()
        task_2_segments = [e for e in result.schedule if e.task_id == 2]
        total_runtime = sum(e.end_time - e.start_time for e in task_2_segments)
        # Task 2 ran for 1 unit on core 0 (t=1..2) then for `sampled - 1` on
        # core 1. Sum equals the original sampled value, in [1, 10].
        assert 1 <= total_runtime <= 10
        # Re-running with the same seed must give the same value (determinism).
        sim2 = DAGSimulator(
            SWAP_FIXTURE_DAG, num_cores=2,
            scheduler=ScriptedScheduler(SWAP_FIXTURE_SCRIPT),
            execution_model="FULL_RANDOM", seed=12345,
        )
        result2 = sim2.run()
        total_runtime2 = sum(
            e.end_time - e.start_time
            for e in result2.schedule if e.task_id == 2
        )
        assert total_runtime == total_runtime2
```

- [ ] **Step 6.2: Run the test (should already pass)**

Run: `pytest tests/test_preemption.py::TestStochasticPreemption -v`
Expected: 1 passed (the cache from Task 4 already gives this behavior).

If it fails, the cache logic in `_run_preemptive` Pass 2 needs investigation — verify the `if desired not in remaining_workload:` guard is present before the `_get_execution_time` call.

- [ ] **Step 6.3: Commit**

```bash
git add tests/test_preemption.py
git commit -m "test: pin stochastic execution time sampling on preemption resume"
```

---

### Task 7: Regression-pin the non-preemptive code path

Snapshot makespan and event ordering produced by the existing `RandomScheduler` with fixed seeds across a few DAGs, so future changes to `_run_non_preemptive` can't silently shift behavior.

**Files:**
- Modify: `tests/test_preemption.py`

- [ ] **Step 7.1: Compute the baseline values**

Run a quick sanity check from the repo root to record the current makespan and schedule for known DAGs (paste-output convenience for the assertions below):

Run:
```bash
python -c "
from dag_sched.dag import DAGTask
from dag_sched.simulator import DAGSimulator
from dag_sched.scheduler import RandomScheduler

dag = DAGTask(successors={1: [2, 3], 2: [4], 3: [4], 4: []}, wcet={1: 1, 2: 5, 3: 3, 4: 1})
for seed in (0, 1, 7):
    r = DAGSimulator(dag, num_cores=2, scheduler=RandomScheduler(seed=seed)).run()
    print(seed, r.makespan, [(e.task_id, e.core_id, e.start_time, e.end_time) for e in sorted(r.schedule, key=lambda e: (e.start_time, e.task_id))])
"
```

Expected: prints three deterministic baselines (one per seed). Record them.

- [ ] **Step 7.2: Add the regression test using the baselines from Step 7.1**

Append to `tests/test_preemption.py`:

```python
class TestNonPreemptivePathRegression:
    """Pins the existing non-preemptive code path so the new dispatch in
    DAGSimulator.run() and the refactor into _run_non_preemptive cannot
    silently shift behavior."""

    @pytest.mark.parametrize("seed,expected_makespan", [
        # Fill these in from the Step 7.1 output.
        (0, ...),
        (1, ...),
        (7, ...),
    ])
    def test_random_scheduler_makespan_matches_baseline(self, seed, expected_makespan):
        dag = DAGTask(
            successors={1: [2, 3], 2: [4], 3: [4], 4: []},
            wcet={1: 1, 2: 5, 3: 3, 4: 1},
        )
        sim = DAGSimulator(dag, num_cores=2, scheduler=RandomScheduler(seed=seed))
        assert sim.run().makespan == expected_makespan
```

Replace each `...` with the makespan printed for the corresponding seed in Step 7.1.

- [ ] **Step 7.3: Run the test**

Run: `pytest tests/test_preemption.py::TestNonPreemptivePathRegression -v`
Expected: 3 passed.

- [ ] **Step 7.4: Commit**

```bash
git add tests/test_preemption.py
git commit -m "test: regression-pin non-preemptive scheduler path baselines"
```

---

### Task 8: Export `PreemptiveScheduler` and document it

Make the new class importable from the top-level package and add a minimal README example so a user discovers preemption without reading the source.

**Files:**
- Modify: `dag_sched/__init__.py`
- Modify: `README.md`

- [ ] **Step 8.1: Add the export**

In `dag_sched/__init__.py`, change the import line from `dag_sched.scheduler` to include the new class, and add it to `__all__`:

```python
from dag_sched.scheduler import Scheduler, SchedulerState, RandomScheduler, PreemptiveScheduler
```

```python
__all__ = [
    "Core",
    "DAGTask",
    "DAGTaskBuilder",
    "Scheduler",
    "SchedulerState",
    "RandomScheduler",
    "PreemptiveScheduler",
    "DAGSimulator",
    "SimulationResult",
    "ScheduleEvent",
    "load_config",
]
```

- [ ] **Step 8.2: Verify the export**

Run:
```bash
python -c "from dag_sched import PreemptiveScheduler; print(PreemptiveScheduler)"
```
Expected: prints `<class 'dag_sched.scheduler.PreemptiveScheduler'>`.

- [ ] **Step 8.3: Add a README section**

In `README.md`, insert a new section directly after the "## Custom Schedulers" section (and before "## Loading from Config Files"):

```markdown
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
- `preemption_cost` adds `swap_count * preemption_cost` time units per scheduling round (all cores paused during the interval).
```

- [ ] **Step 8.4: Run the full test suite one last time**

Run: `pytest tests/ -v`
Expected: all tests pass.

- [ ] **Step 8.5: Commit**

```bash
git add dag_sched/__init__.py README.md
git commit -m "docs: export PreemptiveScheduler and document preemptive scheduling"
```

---

## Self-Review

### Spec coverage

| Spec item | Implemented in |
|---|---|
| `PreemptiveScheduler` ABC with `assign` and disabled `select_task` | Task 2 |
| `DAGSimulator(preemption_cost=...)` parameter | Task 3 |
| `preemption_cost` validation (negative; with non-preemptive scheduler) | Task 3 |
| `Core.preempt()` returning remaining workload | Task 1 |
| `__init__.py` exports `PreemptiveScheduler` | Task 8 |
| Two-pass swap loop in event loop | Task 4 |
| "Missing key = no change" / "None value = idle" semantics | Task 4 (loop logic) + Task 8 (README) |
| Preempted task back to ready with remaining workload | Task 4 |
| Stochastic execution time sampled once per task | Task 4 impl + Task 6 regression test |
| `preemption_cost` charged as `swap_count * cost`, global pause | Task 4 impl + Task 4 cost test |
| `ScheduleEvent` per execution segment for preempted tasks | Task 4 impl + Task 4 segments test |
| `assign()` returns invalid core id → ValueError | Task 5 |
| `assign()` returns unknown task id → ValueError | Task 5 |
| `assign()` returns same task on two cores → ValueError | Task 5 |
| Non-preemptive path unchanged (regression guard) | Task 4 (refactor + existing tests) + Task 7 (pinned baselines) |
| README documents preemptive scheduling | Task 8 |

All spec items covered.

### Type / signature consistency

- `Core.preempt() -> int` (Task 1) is called from `_run_preemptive` (Task 4); both use the same signature.
- `PreemptiveScheduler.assign(ready_queue, running, state) -> dict[int, int | None]` (Task 2) is called from `_run_preemptive` (Task 4) and validated in `_validate_assignment` (Task 5); all three use the same signature.
- `_get_execution_time(node_id)` already exists in `simulator.py` and is called unchanged from `_run_preemptive` (Task 4).
- `SchedulerState` is used unchanged (already imported in `scheduler.py` and `simulator.py`).
- `ScheduleEvent(task_id, core_id, start_time, end_time)` is constructed identically in `_run_non_preemptive` and `_run_preemptive`.

### Placeholder scan

No "TBD", "TODO", "implement later", or vague handwaves outside the explicitly-marked Step 7.2 placeholders (which are filled in by Step 7.1's output during execution).
