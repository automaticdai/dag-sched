"""Processing unit that executes jobs."""

from __future__ import annotations


class Core:
    """A single processing core that can execute one job at a time."""

    def __init__(self) -> None:
        self._idle: bool = True
        self._workload: int = 0
        self._job_id: int | None = None
        self._idle_count: int = 0

    def is_idle(self) -> bool:
        return self._idle

    def get_workload(self) -> int:
        return self._workload

    def get_running_task(self) -> int | None:
        return self._job_id if not self._idle else None

    def get_idle_count(self) -> int:
        return self._idle_count

    def assign(self, job_id: int, execution_time: int) -> None:
        self._job_id = job_id
        self._workload = execution_time
        self._idle = False

    def execute(self, t: int) -> tuple[int | None, bool]:
        """Execute current job for *t* time units.

        Returns (job_id, finished). If idle, accumulates idle time.
        """
        if self._idle:
            self._idle_count += t
            return (None, False)

        self._workload -= t
        finished = self._workload <= 0
        if finished:
            self._workload = 0
            self._idle = True
        return (self._job_id, finished)
