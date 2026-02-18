import math
import pytest
from dag_sched.simulator import DAGSimulator, SimulationResult
from dag_sched.dag import DAGTask
from dag_sched.scheduler import RandomScheduler, Scheduler, SchedulerState


LINEAR_G = {1: [2], 2: [3], 3: []}
LINEAR_C = {1: 1, 2: 5, 3: 1}

DIAMOND_G = {1: [2, 3], 2: [4], 3: [4], 4: []}
DIAMOND_C = {1: 1, 2: 5, 3: 3, 4: 1}


class TestLinearDAG:
    def test_makespan_single_core(self):
        dag = DAGTask(LINEAR_G, LINEAR_C)
        sim = DAGSimulator(dag, num_cores=1, scheduler=RandomScheduler(seed=0))
        result = sim.run()
        assert result.makespan == 7

    def test_makespan_two_cores_same_as_one(self):
        dag = DAGTask(LINEAR_G, LINEAR_C)
        sim = DAGSimulator(dag, num_cores=2, scheduler=RandomScheduler(seed=0))
        result = sim.run()
        assert result.makespan == 7


class TestDiamondDAG:
    def test_makespan_one_core(self):
        dag = DAGTask(DIAMOND_G, DIAMOND_C)
        sim = DAGSimulator(dag, num_cores=1, scheduler=RandomScheduler(seed=0))
        result = sim.run()
        assert result.makespan == 10

    def test_makespan_two_cores(self):
        dag = DAGTask(DIAMOND_G, DIAMOND_C)
        sim = DAGSimulator(dag, num_cores=2, scheduler=RandomScheduler(seed=0))
        result = sim.run()
        assert result.makespan == 7


class TestSimulationResult:
    def test_schedule_events_present(self):
        dag = DAGTask(LINEAR_G, LINEAR_C)
        sim = DAGSimulator(dag, num_cores=1, scheduler=RandomScheduler(seed=0))
        result = sim.run()
        assert len(result.schedule) == 3
        scheduled_nodes = {e.task_id for e in result.schedule}
        assert scheduled_nodes == {1, 2, 3}

    def test_core_utilization(self):
        dag = DAGTask(LINEAR_G, LINEAR_C)
        sim = DAGSimulator(dag, num_cores=2, scheduler=RandomScheduler(seed=0))
        result = sim.run()
        assert len(result.core_utilization) == 2
        assert result.core_utilization[0] == 1.0
        assert result.core_utilization[1] == 0.0


class TestExecutionModels:
    def test_wcet_is_deterministic(self):
        dag = DAGTask(DIAMOND_G, DIAMOND_C)
        results = []
        for _ in range(5):
            sim = DAGSimulator(dag, num_cores=2, scheduler=RandomScheduler(seed=0), execution_model="WCET")
            results.append(sim.run().makespan)
        assert len(set(results)) == 1

    def test_full_random_varies(self):
        dag = DAGTask(DIAMOND_G, DIAMOND_C)
        results = []
        for i in range(20):
            sim = DAGSimulator(dag, num_cores=2, scheduler=RandomScheduler(seed=i), execution_model="FULL_RANDOM", seed=i)
            results.append(sim.run().makespan)
        assert min(results) <= max(results)

    def test_half_random_bounded(self):
        dag = DAGTask(DIAMOND_G, DIAMOND_C)
        for i in range(20):
            sim = DAGSimulator(dag, num_cores=1, scheduler=RandomScheduler(seed=i), execution_model="HALF_RANDOM", seed=i)
            result = sim.run()
            min_total = sum(math.ceil(c / 2) for c in DIAMOND_C.values())
            max_total = sum(DIAMOND_C.values())
            assert min_total <= result.makespan <= max_total


class TestCustomScheduler:
    def test_highest_wcet_first(self):
        class HighestWCETScheduler(Scheduler):
            def select_task(self, ready_queue, state):
                return max(ready_queue, key=lambda t: state.dag.wcet[t])

        dag = DAGTask(DIAMOND_G, DIAMOND_C)
        sim = DAGSimulator(dag, num_cores=1, scheduler=HighestWCETScheduler())
        result = sim.run()
        assert result.makespan == 10
        order = [e.task_id for e in sorted(result.schedule, key=lambda e: e.start_time)]
        assert order == [1, 2, 3, 4]
