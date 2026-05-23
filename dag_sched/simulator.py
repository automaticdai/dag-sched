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
        if isinstance(self.scheduler, PreemptiveScheduler):
            return self._run_preemptive()
        return self._run_non_preemptive()

    def _init_state(self) -> tuple[
        int, list[Core], list[ScheduleEvent],
        dict[int, tuple[int, int]],
        list[int], list[int], set[int],
    ]:
        """Initialize simulator state.

        Returns (t, cores, schedule, task_start, w_queue, r_queue, f_set).
        """
        cores = [Core() for _ in range(self.num_cores)]
        w_queue = list(self.dag.vertices)
        r_queue: list[int] = []
        f_set: set[int] = set()
        source = self.dag.source
        r_queue.append(source)
        w_queue.remove(source)
        return 0, cores, [], {}, w_queue, r_queue, f_set

    def _build_result(
        self, t: int, cores: list[Core], schedule: list[ScheduleEvent]
    ) -> SimulationResult:
        utilization = []
        for c in range(self.num_cores):
            if t > 0:
                idle_time = cores[c].get_idle_count()
                utilization.append(1.0 - idle_time / t)
            else:
                utilization.append(0.0)
        return SimulationResult(
            makespan=t, schedule=schedule, core_utilization=utilization,
        )

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

    def _run_non_preemptive(self) -> SimulationResult:
        t, cores, schedule, task_start, w_queue, r_queue, f_set = self._init_state()

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

        return self._build_result(t, cores, schedule)

    def _run_preemptive(self) -> SimulationResult:
        t, cores, schedule, task_start, w_queue, r_queue, f_set = self._init_state()
        remaining_workload: dict[int, int] = {}

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
            self._validate_assignment(assignment, list(r_queue), dict(running))

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

            # 4. Cost interval: charged only when at least one running task was
            #    actually displaced (not idle→task dispatches).
            preempted_count = sum(
                1 for c, _ in swap_cores if running.get(c) is not None
            )
            if preempted_count > 0:
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

        return self._build_result(t, cores, schedule)
