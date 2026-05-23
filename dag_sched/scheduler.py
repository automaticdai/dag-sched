"""Scheduler interface and built-in implementations."""

from __future__ import annotations

import random as _random
from abc import ABC, abstractmethod
from dataclasses import dataclass

from dag_sched.core import Core
from dag_sched.dag import DAGTask


@dataclass(frozen=True)
class SchedulerState:
    """Read-only snapshot of simulator state passed to the scheduler."""

    dag: DAGTask
    cores: list[Core]
    current_time: int
    finished_tasks: set[int]


class Scheduler(ABC):
    """Base class for scheduling algorithms."""

    @abstractmethod
    def select_task(self, ready_queue: list[int], state: SchedulerState) -> int:
        """Pick a task ID from the ready queue to schedule next."""
        ...

    def on_task_complete(self, task_id: int, time: int) -> None:
        """Optional hook called when a task finishes."""
        pass


class RandomScheduler(Scheduler):
    """Picks a random task from the ready queue."""

    def __init__(self, seed: int | None = None) -> None:
        self._rng = _random.Random(seed)

    def select_task(self, ready_queue: list[int], state: SchedulerState) -> int:
        return self._rng.choice(ready_queue)


class PreemptiveScheduler(Scheduler):
    """Scheduler that produces a full core→task assignment each event boundary.

    The simulator calls `assign(...)` at every event boundary. The returned
    dict maps `core_id -> task_id_or_None`:
      - missing key for a core: "no change" (don't preempt; don't dispatch)
      - core_id: None         : "idle this core" (preempt without replacement)
      - core_id: task_id      : "this task should run here" (dispatch, or
                                 preempt the current task if different)

    `assign` receives:
      - ready_queue: list of task_ids currently waiting to run
      - running:     dict mapping core_id -> task_id for *non-idle* cores;
                     idle cores are absent from this dict
      - state:       a SchedulerState snapshot (dag, cores, current_time,
                     finished_tasks)

    A preempted task goes back to the ready queue with its remaining workload.
    The inherited `on_task_complete(task_id, time)` hook is called when a task
    finishes (not when it is preempted).
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
