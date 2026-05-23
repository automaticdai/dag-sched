"""Tests for preemptive scheduling support."""
from __future__ import annotations

import pytest

from dag_sched.core import Core
from dag_sched.dag import DAGTask
from dag_sched.scheduler import PreemptiveScheduler, RandomScheduler, Scheduler
from dag_sched.simulator import DAGSimulator


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


class TestCoreAddIdleTime:
    def test_add_idle_time_increments_idle_count(self):
        c = Core()
        c.add_idle_time(5)
        assert c.get_idle_count() == 5

    def test_add_idle_time_accumulates(self):
        c = Core()
        c.add_idle_time(3)
        c.add_idle_time(2)
        assert c.get_idle_count() == 5

    def test_add_idle_time_negative_raises(self):
        c = Core()
        with pytest.raises(ValueError):
            c.add_idle_time(-1)


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


def _minimal_dag() -> DAGTask:
    """Two-node line DAG; passes the simulator's single-source/single-sink check."""
    return DAGTask(successors={1: [2], 2: []}, wcet={1: 1, 2: 1})


class TestPreemptionCostValidation:
    def test_negative_preemption_cost_raises(self):
        class S(PreemptiveScheduler):
            def assign(self, ready_queue, running, state):
                return {}
        with pytest.raises(ValueError, match="preemption_cost"):
            DAGSimulator(
                _minimal_dag(), num_cores=1,
                scheduler=S(),
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
        sim = DAGSimulator(
            _minimal_dag(), num_cores=1,
            scheduler=RandomScheduler(),
            preemption_cost=0,
        )
        assert sim.preemption_cost == 0

    def test_positive_cost_with_preemptive_scheduler_ok(self):
        class S(PreemptiveScheduler):
            def assign(self, ready_queue, running, state):
                return {}
        sim = DAGSimulator(
            _minimal_dag(), num_cores=1,
            scheduler=S(),
            preemption_cost=5,
        )
        assert sim.preemption_cost == 5


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

    def test_preemption_cost_charges_idle_time_to_all_cores(self):
        # With preemption_cost=5 and 2 swaps in round 3, 10 units of cost
        # are charged. All cores should accumulate that as idle time so
        # utilization is computed correctly.
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
        r0 = sim0.run()
        r5 = sim5.run()
        # The 10 units of cost (2 swaps * 5) should show up as additional
        # idle time on both cores, so utilization * makespan is preserved
        # (total work done is the same; only the makespan grew).
        busy_time_0_no_cost = sum(r0.core_utilization[c] * r0.makespan for c in range(2))
        busy_time_5_with_cost = sum(r5.core_utilization[c] * r5.makespan for c in range(2))
        # Allow tiny floating-point slack.
        assert abs(busy_time_0_no_cost - busy_time_5_with_cost) < 1e-9

    def test_assignment_none_value_preempts_and_idles_core(self):
        # 3 cores, DAG: 1 -> {2, 3, 4} -> 5.
        # wcet: 1=1, 2=10, 3=8, 4=6, 5=1.
        # Timeline:
        #   t=0: call 1 -> {0: 1}; core 0 runs task 1.
        #   t=1: call 2 -> {0: 2, 1: 3, 2: 4}; task 1 done; all cores busy.
        #   t=7: task 4 finishes (wcet=6); call 3 -> {0: None}; preempt task 2
        #        (3 units used, 7 remaining); core 0 goes idle.
        #        Core 1 still running task 3 (1 unit left). Simulation advances.
        #   t=8: task 3 finishes; call 4 -> {0: 2}; dispatch task 2 (7 units).
        #   t=15: task 2 finishes; call 5 -> {0: 5}; dispatch task 5.
        #   t=16: task 5 done; DAG complete.
        dag = DAGTask(
            successors={1: [2, 3, 4], 2: [5], 3: [5], 4: [5], 5: []},
            wcet={1: 1, 2: 10, 3: 8, 4: 6, 5: 1},
        )

        class IdleCore0AfterTask4(PreemptiveScheduler):
            def __init__(self):
                self.calls = 0
            def assign(self, ready_queue, running, state):
                self.calls += 1
                if self.calls == 1:        # t=0: dispatch source
                    return {0: 1}
                if self.calls == 2:        # t=1: dispatch 2, 3, 4
                    return {0: 2, 1: 3, 2: 4}
                if self.calls == 3:        # t=7: task 4 done; idle core 0 (preempt 2)
                    return {0: None}
                if self.calls == 4:        # t=8: task 3 done; redispatch task 2
                    return {0: 2}
                if 5 in ready_queue:       # t=15: task 2 done; dispatch task 5
                    return {0: 5}
                return {}

        sim = DAGSimulator(dag, num_cores=3, scheduler=IdleCore0AfterTask4())
        result = sim.run()
        # Task 2 should appear twice: first segment ended at t=7 (preempted),
        # second segment resumes at t=8 (after task 3 finishes).
        task_2_events = [e for e in result.schedule if e.task_id == 2]
        assert len(task_2_events) == 2
        # First segment runs t=1..7 on core 0 (6 units of the 10, then preempted).
        assert task_2_events[0].start_time == 1
        assert task_2_events[0].end_time == 7


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
        greedy = GreedyPreemptiveScheduler()

        class Bad(PreemptiveScheduler):
            def assign(self, ready_queue, running, state):
                if 2 in ready_queue and 3 in ready_queue:
                    return {0: 2, 1: 2}
                return greedy.assign(ready_queue, running, state)

        sim = DAGSimulator(dag, num_cores=2, scheduler=Bad())
        with pytest.raises(ValueError, match="duplicate"):
            sim.run()

    def test_migration_without_freeing_source_core_raises(self):
        # 2 cores, task 2 runs long; scheduler tries to migrate it from
        # core 0 to core 1 without freeing core 0.
        dag = DAGTask(
            successors={1: [2, 3], 2: [4], 3: [4], 4: []},
            wcet={1: 1, 2: 10, 3: 1, 4: 1},
        )

        class Bad(PreemptiveScheduler):
            def __init__(self):
                self.calls = 0

            def assign(self, ready_queue, running, state):
                self.calls += 1
                # Round 1: dispatch source.
                if self.calls == 1:
                    return {0: 1}
                # Round 2: dispatch task 2 to core 0, task 3 to core 1.
                if self.calls == 2:
                    return {0: 2, 1: 3}
                # Round 3: task 3 just finished; bad scheduler tries to
                # migrate task 2 from core 0 to core 1 without freeing core 0.
                return {1: 2}

        sim = DAGSimulator(dag, num_cores=2, scheduler=Bad())
        with pytest.raises(ValueError, match="migrate"):
            sim.run()


class TestExecutionTimeSampledOncePerTask:
    """Verifies that `_get_execution_time` is called at most once per task,
    even when the task is preempted and later resumed. This is the property
    that makes stochastic execution models (HALF_RANDOM, FULL_RANDOM) yield
    physically meaningful results — a task's true runtime is a property of
    the job, not of the scheduler's preemption decisions."""

    def test_preempted_task_is_not_resampled_on_resume(self):
        # SWAP_FIXTURE preempts task 2 after 1 unit (on core 0) and resumes
        # it on core 1. Under WCET the actual values don't change; what we
        # check is that the simulator caches the workload instead of asking
        # `_get_execution_time` for a fresh sample on resume.
        sim = DAGSimulator(
            SWAP_FIXTURE_DAG, num_cores=2,
            scheduler=ScriptedScheduler(SWAP_FIXTURE_SCRIPT),
        )
        sample_counts: dict[int, int] = {}
        original_get_execution_time = sim._get_execution_time

        def counting_get_execution_time(task_id: int) -> int:
            sample_counts[task_id] = sample_counts.get(task_id, 0) + 1
            return original_get_execution_time(task_id)

        sim._get_execution_time = counting_get_execution_time  # type: ignore[method-assign]
        sim.run()

        # All 5 tasks were dispatched at least once.
        assert set(sample_counts.keys()) == {1, 2, 3, 4, 5}
        # Crucially: task 2 was dispatched twice (preempt + resume) but
        # `_get_execution_time` was called only once — the cached remaining
        # workload was used on resume.
        assert sample_counts[2] == 1
        # All other tasks are dispatched exactly once.
        for task_id in (1, 3, 4, 5):
            assert sample_counts[task_id] == 1


class TestNonPreemptivePathRegression:
    """Pins the existing non-preemptive code path so the new dispatch in
    DAGSimulator.run() and the refactor into _run_non_preemptive cannot
    silently shift behavior."""

    @pytest.mark.parametrize("seed,expected_makespan", [
        # Seeds 0 and 2 produce distinct makespans (8 vs 9), confirming
        # the test catches behavioral drift rather than just trivial regressions.
        (0, 8),
        (2, 9),
        (12, 9),
    ])
    def test_random_scheduler_makespan_matches_baseline(self, seed, expected_makespan):
        dag = DAGTask(
            successors={1: [2, 3, 4, 5], 2: [6], 3: [6], 4: [6], 5: [6], 6: []},
            wcet={1: 1, 2: 3, 3: 2, 4: 4, 5: 2, 6: 1},
        )
        sim = DAGSimulator(dag, num_cores=2, scheduler=RandomScheduler(seed=seed))
        assert sim.run().makespan == expected_makespan
