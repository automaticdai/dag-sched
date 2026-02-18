from dag_sched.core import Core


class TestCore:
    def test_new_core_is_idle(self):
        core = Core()
        assert core.is_idle() is True
        assert core.get_workload() == 0

    def test_assign_job_makes_core_busy(self):
        core = Core()
        core.assign(job_id=1, execution_time=10)
        assert core.is_idle() is False
        assert core.get_workload() == 10

    def test_execute_reduces_workload(self):
        core = Core()
        core.assign(job_id=1, execution_time=10)
        job_id, finished = core.execute(3)
        assert job_id == 1
        assert finished is False
        assert core.get_workload() == 7
        assert core.is_idle() is False

    def test_execute_finishes_job(self):
        core = Core()
        core.assign(job_id=1, execution_time=10)
        job_id, finished = core.execute(10)
        assert job_id == 1
        assert finished is True
        assert core.is_idle() is True
        assert core.get_workload() == 0

    def test_execute_idle_core_accumulates_idle_time(self):
        core = Core()
        core.execute(5)
        assert core.get_idle_count() == 5
        core.execute(3)
        assert core.get_idle_count() == 8

    def test_get_running_task(self):
        core = Core()
        assert core.get_running_task() is None
        core.assign(job_id=42, execution_time=5)
        assert core.get_running_task() == 42
