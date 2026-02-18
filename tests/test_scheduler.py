import pytest
from dag_sched.scheduler import Scheduler, RandomScheduler, SchedulerState
from dag_sched.dag import DAGTask
from dag_sched.core import Core


DIAMOND_G = {1: [2, 3], 2: [4], 3: [4], 4: []}
DIAMOND_C = {1: 1, 2: 5, 3: 3, 4: 1}


class TestSchedulerABC:
    def test_cannot_instantiate_abstract(self):
        with pytest.raises(TypeError):
            Scheduler()


class TestRandomScheduler:
    def test_selects_from_ready_queue(self):
        dag = DAGTask(DIAMOND_G, DIAMOND_C)
        state = SchedulerState(dag=dag, cores=[Core()], current_time=0, finished_tasks=set())
        sched = RandomScheduler(seed=42)
        task_id = sched.select_task([2, 3], state)
        assert task_id in [2, 3]

    def test_deterministic_with_seed(self):
        dag = DAGTask(DIAMOND_G, DIAMOND_C)
        state = SchedulerState(dag=dag, cores=[Core()], current_time=0, finished_tasks=set())
        results = []
        for _ in range(10):
            sched = RandomScheduler(seed=42)
            results.append(sched.select_task([2, 3], state))
        assert len(set(results)) == 1
