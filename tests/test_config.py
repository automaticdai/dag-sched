import json
import pytest
from dag_sched.config import load_config
from dag_sched.simulator import DAGSimulator
from dag_sched.scheduler import RandomScheduler


class TestLoadConfig:
    def test_load_json_config(self, tmp_path):
        cfg = {
            "dag": {
                "nodes": {"1": 1, "2": 5, "3": 3, "4": 1},
                "edges": [[1, 2], [1, 3], [2, 4], [3, 4]]
            },
            "num_cores": 2,
            "execution_model": "WCET"
        }
        path = tmp_path / "sim.json"
        path.write_text(json.dumps(cfg))
        sim = load_config(str(path), scheduler=RandomScheduler(seed=0))
        assert isinstance(sim, DAGSimulator)
        result = sim.run()
        assert result.makespan == 7

    def test_load_yaml_config(self, tmp_path):
        content = """
dag:
  nodes:
    1: 1
    2: 5
    3: 3
    4: 1
  edges:
    - [1, 2]
    - [1, 3]
    - [2, 4]
    - [3, 4]
num_cores: 2
execution_model: WCET
"""
        path = tmp_path / "sim.yaml"
        path.write_text(content)
        sim = load_config(str(path), scheduler=RandomScheduler(seed=0))
        result = sim.run()
        assert result.makespan == 7

    def test_defaults(self, tmp_path):
        cfg = {
            "dag": {
                "nodes": {"1": 1, "2": 5, "3": 1},
                "edges": [[1, 2], [2, 3]]
            }
        }
        path = tmp_path / "sim.json"
        path.write_text(json.dumps(cfg))
        sim = load_config(str(path), scheduler=RandomScheduler(seed=0))
        result = sim.run()
        assert result.makespan == 7
