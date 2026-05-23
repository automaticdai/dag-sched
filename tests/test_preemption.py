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
