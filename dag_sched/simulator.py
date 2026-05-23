"""DAG scheduling simulator."""

from __future__ import annotations

import math
import random as _random
from dataclasses import dataclass, field

from dag_sched.core import Core
from dag_sched.dag import DAGTask
from dag_sched.scheduler import PreemptiveScheduler, Scheduler, SchedulerState


EXECUTION_MODELS = ("WCET", "BCET", "HALF_RANDOM", "FULL_RANDOM")
T_MAX = 1_000_000_000


@dataclass
class ScheduleEvent:
    """Record of a task execution on a core."""

    task_id: int
    core_id: int
    start_time: int
    end_time: int


@dataclass
class SimulationResult:
    """Result of a simulation run."""

    makespan: int
    schedule: list[ScheduleEvent] = field(default_factory=list)
    core_utilization: list[float] = field(default_factory=list)


class DAGSimulator:
    """Event-driven DAG scheduling simulator."""

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
        if preemption_cost > 0 and not isinstance(scheduler, PreemptiveScheduler):
            raise ValueError(
                "preemption_cost > 0 requires a PreemptiveScheduler; "
                "either use a PreemptiveScheduler or set preemption_cost=0"
            )
        self.dag = dag
        self.num_cores = num_cores
        self.scheduler = scheduler
        self.execution_model = execution_model
        self.preemption_cost = preemption_cost
        self._rng = _random.Random(seed)

    def _get_execution_time(self, node_id: int) -> int:
        wcet = self.dag.wcet[node_id]
        if self.execution_model == "WCET":
            return wcet
        elif self.execution_model == "BCET":
            return 1
        elif self.execution_model == "HALF_RANDOM":
            return self._rng.randint(math.ceil(wcet / 2), wcet)
        elif self.execution_model == "FULL_RANDOM":
            return self._rng.randint(1, wcet)
        return wcet

    def run(self) -> SimulationResult:
        t = 0
        cores = [Core() for _ in range(self.num_cores)]
        schedule: list[ScheduleEvent] = []
        task_start: dict[int, tuple[int, int]] = {}

        w_queue = list(self.dag.vertices)
        r_queue: list[int] = []
        f_set: set[int] = set()

        source = self.dag.source
        r_queue.append(source)
        w_queue.remove(source)

        while t < T_MAX:
            newly_ready = []
            for node in w_queue:
                if all(p in f_set for p in self.dag.predecessors[node]):
                    newly_ready.append(node)
            for node in newly_ready:
                r_queue.append(node)
                w_queue.remove(node)

            state = SchedulerState(
                dag=self.dag,
                cores=cores,
                current_time=t,
                finished_tasks=set(f_set),
            )
            for m in range(self.num_cores):
                if cores[m].is_idle() and r_queue:
                    task_id = self.scheduler.select_task(list(r_queue), state)
                    exec_time = self._get_execution_time(task_id)
                    cores[m].assign(job_id=task_id, execution_time=exec_time)
                    r_queue.remove(task_id)
                    task_start[task_id] = (m, t)

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
            for m in range(self.num_cores):
                task_id, finished = cores[m].execute(int(sp))
                if finished:
                    f_set.add(task_id)
                    core_id, start_time = task_start[task_id]
                    schedule.append(ScheduleEvent(
                        task_id=task_id,
                        core_id=core_id,
                        start_time=start_time,
                        end_time=t,
                    ))
                    self.scheduler.on_task_complete(task_id, t)

            if f_set == set(self.dag.vertices):
                break

        makespan = t
        utilization = []
        for m in range(self.num_cores):
            if makespan > 0:
                idle_time = cores[m].get_idle_count()
                utilization.append(1.0 - idle_time / makespan)
            else:
                utilization.append(0.0)

        return SimulationResult(
            makespan=makespan,
            schedule=schedule,
            core_utilization=utilization,
        )
