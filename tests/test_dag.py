import json
import pytest
from dag_sched.dag import DAGTask


DIAMOND_G = {1: [2, 3], 2: [4], 3: [4], 4: []}
DIAMOND_C = {1: 1, 2: 5, 3: 3, 4: 1}


class TestDAGTaskDirect:
    def test_create_from_dicts(self):
        dag = DAGTask(DIAMOND_G, DIAMOND_C)
        assert dag.vertices == [1, 2, 3, 4]
        assert dag.wcet == DIAMOND_C
        assert dag.successors == DIAMOND_G
        assert dag.predecessors[1] == []
        assert set(dag.predecessors[4]) == {2, 3}

    def test_source_and_sink(self):
        dag = DAGTask(DIAMOND_G, DIAMOND_C)
        assert dag.source == 1
        assert dag.sink == 4

    def test_validate_catches_cycle(self):
        cyclic = {1: [2], 2: [3], 3: [1]}
        c = {1: 1, 2: 1, 3: 1}
        with pytest.raises(ValueError, match="cycle"):
            DAGTask(cyclic, c)

    def test_validate_catches_missing_wcet(self):
        g = {1: [2], 2: []}
        c = {1: 1}
        with pytest.raises(ValueError, match="wcet"):
            DAGTask(g, c)

    def test_validate_catches_multiple_sources(self):
        g = {1: [3], 2: [3], 3: []}
        c = {1: 1, 2: 1, 3: 1}
        with pytest.raises(ValueError, match="source"):
            DAGTask(g, c)

    def test_validate_catches_multiple_sinks(self):
        g = {1: [2, 3], 2: [], 3: []}
        c = {1: 1, 2: 1, 3: 1}
        with pytest.raises(ValueError, match="sink"):
            DAGTask(g, c)


class TestDAGTaskBuilder:
    def test_builder_api(self):
        dag = DAGTask.builder()
        dag.add_node(1, wcet=1)
        dag.add_node(2, wcet=5)
        dag.add_node(3, wcet=3)
        dag.add_node(4, wcet=1)
        dag.add_edge(1, 2)
        dag.add_edge(1, 3)
        dag.add_edge(2, 4)
        dag.add_edge(3, 4)
        result = dag.build()
        assert result.vertices == [1, 2, 3, 4]
        assert result.wcet[2] == 5


class TestDAGTaskFromJSON:
    def test_from_json(self, tmp_path):
        data = {
            "nodes": {"1": 1, "2": 5, "3": 3, "4": 1},
            "edges": [[1, 2], [1, 3], [2, 4], [3, 4]]
        }
        path = tmp_path / "dag.json"
        path.write_text(json.dumps(data))
        dag = DAGTask.from_json(str(path))
        assert dag.vertices == [1, 2, 3, 4]
        assert dag.wcet[2] == 5


class TestDAGTaskFromYAML:
    def test_from_yaml(self, tmp_path):
        content = """
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
"""
        path = tmp_path / "dag.yaml"
        path.write_text(content)
        dag = DAGTask.from_yaml(str(path))
        assert dag.vertices == [1, 2, 3, 4]
        assert dag.wcet[3] == 3
