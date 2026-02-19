from __future__ import annotations

from app.detection.preprocess import PreprocessResult


def detect_shell_paths(
    ctx: PreprocessResult,
    min_length: int = 4,
    max_hops: int = 8,
) -> tuple[list[dict[str, object]], set[str]]:
    def is_shell(node: str) -> bool:
        return (
            ctx.total_tx_count.get(node, 0) in (2, 3)
            and ctx.in_degree.get(node, 0) == 1
            and ctx.out_degree.get(node, 0) == 1
        )

    seen_paths: set[tuple[str, ...]] = set()
    rings: list[dict[str, object]] = []
    shell_nodes: set[str] = set()

    for source in ctx.accounts:
        if is_shell(source):
            continue
        for first in ctx.adjacency.get(source, ()):
            if not is_shell(first):
                continue
            path = [source, first]
            visited = {source, first}
            current = first

            while True:
                if len(path) - 1 >= max_hops:
                    break
                next_nodes = ctx.adjacency.get(current, ())
                if not next_nodes:
                    break
                nxt = next_nodes[0]
                if nxt in visited:
                    break
                path.append(nxt)
                visited.add(nxt)
                if not is_shell(nxt):
                    break
                current = nxt

            if len(path) < min_length:
                continue
            if is_shell(path[-1]):
                continue

            path_key = tuple(path)
            if path_key in seen_paths:
                continue
            seen_paths.add(path_key)

            members = tuple(sorted(set(path)))
            rings.append(
                {
                    "member_accounts": list(members),
                    "pattern_type": "shell",
                    "path": path_key,
                }
            )
            for node in path[1:-1]:
                shell_nodes.add(node)

    rings.sort(key=lambda item: tuple(item["member_accounts"]))
    return rings, shell_nodes
