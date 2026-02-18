"""Configuration file loader for DAGSimulator."""

from __future__ import annotations

import json
from pathlib import Path

from dag_sched.dag import DAGTask
from dag_sched.scheduler import Scheduler
from dag_sched.simulator import DAGSimulator


def load_config(
    path: str,
    scheduler: Scheduler,
    seed: int | None = None,
) -> DAGSimulator:
    """Load a simulation configuration from a JSON or YAML file."""
    p = Path(path)
    suffix = p.suffix.lower()

    if suffix in (".yaml", ".yml"):
        import yaml
        with open(p) as f:
            data = yaml.safe_load(f)
    else:
        with open(p) as f:
            data = json.load(f)

    dag = DAGTask.from_dict(data["dag"])
    num_cores = data.get("num_cores", 1)
    execution_model = data.get("execution_model", "WCET")

    return DAGSimulator(
        dag=dag,
        num_cores=num_cores,
        scheduler=scheduler,
        execution_model=execution_model,
        seed=seed,
    )
