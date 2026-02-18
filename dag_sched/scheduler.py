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
