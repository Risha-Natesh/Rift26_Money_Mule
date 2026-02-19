from __future__ import annotations

from collections import defaultdict

from app.detection.preprocess import PreprocessResult


def _canonicalize_cycle(path: list[str]) -> tuple[str, ...]:
    cycle = tuple(path)
    if not cycle:
        return cycle
    smallest = min(cycle)
    idx = cycle.index(smallest)
    return cycle[idx:] + cycle[:idx]


def detect_cycles(
    ctx: PreprocessResult,
    min_cycle_len: int = 3,
    max_cycle_len: int = 5,
) -> tuple[list[dict[str, object]], dict[str, set[str]]]:
    cycles_by_set: dict[frozenset[str], tuple[str, ...]] = {}

    def register_cycle(cycle: tuple[str, ...]) -> None:
        if not cycle:
            return
        members_set = frozenset(cycle)

        for existing_set in list(cycles_by_set):
            if members_set == existing_set:
                return
            if members_set < existing_set:
                return

        for existing_set in list(cycles_by_set):
            if members_set > existing_set:
                cycles_by_set.pop(existing_set, None)

        cycles_by_set[members_set] = cycle

    def bounded_dfs(
        start: str,
        current: str,
        path: list[str],
        visited: set[str],
        adjacency: dict[str, tuple[str, ...]],
    ) -> None:
        for nxt in adjacency.get(current, ()):
            if nxt == start:
                cycle_len = len(path)
                if min_cycle_len <= cycle_len <= max_cycle_len:
                    register_cycle(_canonicalize_cycle(path))
                continue

            if len(path) >= max_cycle_len:
                continue
            if nxt in visited:
                continue
            if nxt < start:
                continue

            visited.add(nxt)
            path.append(nxt)
            bounded_dfs(start, nxt, path, visited, adjacency)
            path.pop()
            visited.remove(nxt)

    for scc_nodes in ctx.sccs:
        if len(scc_nodes) < min_cycle_len:
            continue
        scc_set = set(scc_nodes)
        adjacency = {
            node: tuple(nbr for nbr in ctx.adjacency.get(node, ()) if nbr in scc_set)
            for node in scc_nodes
        }
        for start in scc_nodes:
            bounded_dfs(start, start, [start], {start}, adjacency)

    cycles: list[dict[str, object]] = []
    account_patterns: dict[str, set[str]] = defaultdict(set)

    for cycle in sorted(cycles_by_set.values()):
        cycle_len = len(cycle)
        cycles.append(
            {
                "member_accounts": list(cycle),
                "cycle_length": cycle_len,
                "pattern_type": "cycle",
                "quality_score": 1.0,
            }
        )
        for account in cycle:
            account_patterns[account].add("cycle")

    return cycles, dict(account_patterns)
