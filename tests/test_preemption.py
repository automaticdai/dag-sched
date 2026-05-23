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
