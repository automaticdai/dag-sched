from dag_sched.graph import (
    find_all_paths,
    find_longest_path,
    find_predecessors,
    find_successors,
    find_ancestors,
    find_descendants,
)

G = {1: [2, 3], 2: [4], 3: [4], 4: []}
WEIGHTS = {1: 1, 2: 5, 3: 3, 4: 1}


class TestFindAllPaths:
    def test_diamond(self):
        paths = find_all_paths(G, 1, 4)
        assert [1, 2, 4] in paths
        assert [1, 3, 4] in paths
        assert len(paths) == 2

    def test_single_node(self):
        paths = find_all_paths({1: []}, 1, 1)
        assert paths == [[1]]


class TestFindLongestPath:
    def test_diamond(self):
        cost, path = find_longest_path(G, 1, 4, WEIGHTS)
        assert cost == 7
        assert path == [1, 2, 4]


class TestPredecessorsSuccessors:
    def test_predecessors(self):
        assert find_predecessors(G, 4) == [2, 3]
        assert find_predecessors(G, 1) == []

    def test_successors(self):
        assert find_successors(G, 1) == [2, 3]
        assert find_successors(G, 4) == []


class TestAncestorsDescendants:
    def test_ancestors(self):
        g = {1: [2, 3, 4], 2: [5], 3: [5], 4: [5], 5: []}
        anc = find_ancestors(g, 5)
        assert sorted(anc) == [1, 2, 3, 4]

    def test_ancestors_of_source(self):
        assert find_ancestors(G, 1) == []

    def test_descendants(self):
        desc = find_descendants(G, 1)
        assert sorted(desc) == [2, 3, 4]

    def test_descendants_of_sink(self):
        assert find_descendants(G, 4) == []
