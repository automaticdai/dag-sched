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
