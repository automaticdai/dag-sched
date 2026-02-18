"""Graph utility functions for DAG analysis."""

from __future__ import annotations


def find_all_paths(
    graph: dict[int, list[int]], start: int, end: int
) -> list[list[int]]:
    """Find all paths from start to end in a DAG."""
    if start == end:
        return [[start]]
    if start not in graph:
        return []
    paths = []
    for neighbor in graph[start]:
        for p in find_all_paths(graph, neighbor, end):
            paths.append([start] + p)
    return paths


def find_longest_path(
    graph: dict[int, list[int]],
    start: int,
    end: int,
    weights: dict[int, int],
) -> tuple[int, list[int]]:
    """Find the longest (critical) path by total weight."""
    paths = find_all_paths(graph, start, end)
    if not paths:
        return (0, [])
    best_cost = -1
    best_path: list[int] = []
    for path in paths:
        cost = sum(weights[v] for v in path)
        if cost > best_cost:
            best_cost = cost
            best_path = path
    return (best_cost, best_path)


def find_predecessors(graph: dict[int, list[int]], node: int) -> list[int]:
    """Find immediate predecessors of a node."""
    return sorted(k for k, succs in graph.items() if node in succs)


def find_successors(graph: dict[int, list[int]], node: int) -> list[int]:
    """Find immediate successors of a node."""
    return list(graph.get(node, []))


def find_ancestors(graph: dict[int, list[int]], node: int) -> list[int]:
    """Find all ancestors (transitive predecessors) of a node."""
    visited: set[int] = set()
    stack = find_predecessors(graph, node)
    while stack:
        v = stack.pop()
        if v not in visited:
            visited.add(v)
            stack.extend(find_predecessors(graph, v))
    return sorted(visited)


def find_descendants(graph: dict[int, list[int]], node: int) -> list[int]:
    """Find all descendants (transitive successors) of a node."""
    visited: set[int] = set()
    stack = list(graph.get(node, []))
    while stack:
        v = stack.pop()
        if v not in visited:
            visited.add(v)
            stack.extend(graph.get(v, []))
    return sorted(visited)
