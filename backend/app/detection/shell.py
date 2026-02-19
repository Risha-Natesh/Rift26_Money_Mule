from __future__ import annotations

from bisect import bisect_left
from collections import defaultdict

import numpy as np
import pandas as pd

from app.detection.preprocess import PreprocessResult


MAX_HOP_GAP_HOURS = 6.0
AMOUNT_TOLERANCE = 0.10
RAPID_FORWARD_HOURS = 1.0
SHORT_HOLDING_HOURS = 1.0


def _build_edge_index(
    ctx: PreprocessResult,
) -> tuple[
    dict[tuple[str, str], list[pd.Timestamp]],
    dict[tuple[str, str], list[tuple[pd.Timestamp, float]]],
]:
    edge_times: dict[tuple[str, str], list[pd.Timestamp]] = defaultdict(list)
    edge_events: dict[tuple[str, str], list[tuple[pd.Timestamp, float]]] = defaultdict(list)

    for sender, receiver, timestamp, amount in ctx.tx_sorted[
        ["sender_id", "receiver_id", "timestamp", "amount"]
    ].itertuples(index=False, name=None):
        key = (str(sender), str(receiver))
        ts = pd.Timestamp(timestamp)
        amt = float(amount)
        edge_times[key].append(ts)
        edge_events[key].append((ts, amt))

    return edge_times, edge_events


def _default_high_volume_threshold(
    ctx: PreprocessResult,
    min_length: int,
) -> float:
    amounts = ctx.tx_sorted["amount"].astype(float).tolist()
    if not amounts:
        return float("inf")
    p90 = float(np.percentile(amounts, 90))
    # Scale by minimum edge count so short normal chains do not trigger by topology alone.
    return max(1000.0, p90 * float(max(1, min_length - 1)))


def _validate_path_events(
    path: tuple[str, ...],
    edge_times: dict[tuple[str, str], list[pd.Timestamp]],
    edge_events: dict[tuple[str, str], list[tuple[pd.Timestamp, float]]],
) -> tuple[bool, list[pd.Timestamp], list[float]]:
    selected_times: list[pd.Timestamp] = []
    selected_amounts: list[float] = []
    max_gap = pd.Timedelta(hours=MAX_HOP_GAP_HOURS)

    prev_ts: pd.Timestamp | None = None
    prev_amt: float | None = None

    for index in range(len(path) - 1):
        edge = (path[index], path[index + 1])
        times = edge_times.get(edge, [])
        events = edge_events.get(edge, [])
        if not times or not events:
            return False, [], []

        start_idx = 0
        if prev_ts is not None:
            start_idx = bisect_left(times, prev_ts)

        chosen_ts: pd.Timestamp | None = None
        chosen_amt: float | None = None

        for ts, amt in events[start_idx:]:
            if prev_ts is not None:
                delta = ts - prev_ts
                if delta > max_gap:
                    break
                if delta < pd.Timedelta(0):
                    continue
            if prev_amt is not None:
                if prev_amt <= 0:
                    return False, [], []
                change_ratio = abs(prev_amt - amt) / prev_amt
                if change_ratio > AMOUNT_TOLERANCE:
                    continue

            chosen_ts = ts
            chosen_amt = amt
            break

        if chosen_ts is None or chosen_amt is None:
            return False, [], []

        selected_times.append(chosen_ts)
        selected_amounts.append(chosen_amt)
        prev_ts = chosen_ts
        prev_amt = chosen_amt

    return True, selected_times, selected_amounts


def _is_rapid_forwarding(
    hop_times: list[pd.Timestamp],
) -> bool:
    if len(hop_times) < 2:
        return False
    rapid_limit = pd.Timedelta(hours=RAPID_FORWARD_HOURS)
    for index in range(1, len(hop_times)):
        if hop_times[index] - hop_times[index - 1] > rapid_limit:
            return False
    return True


def _build_suspicious_proxy_nodes(ctx: PreprocessResult) -> set[str]:
    cycle_like_nodes = {
        node for component in ctx.sccs if len(component) >= 3 for node in component
    }

    smurf_like_nodes: set[str] = set()
    volume_threshold = max(0.0, ctx.small_amount_threshold * 10.0)
    for account in ctx.accounts:
        if ctx.in_degree.get(account, 0) < 10:
            continue
        if ctx.inbound_volume.get(account, 0.0) < volume_threshold:
            continue
        if ctx.median_holding_hours.get(account, 1e9) > 48.0:
            continue
        smurf_like_nodes.add(account)

    return cycle_like_nodes.union(smurf_like_nodes)


def _has_short_holding(path: tuple[str, ...], ctx: PreprocessResult) -> bool:
    intermediaries = path[1:-1]
    if not intermediaries:
        return False
    for node in intermediaries:
        if ctx.median_holding_hours.get(node, 1e9) > SHORT_HOLDING_HOURS:
            return False
    return True


def detect_shell_paths(
    ctx: PreprocessResult,
    min_length: int = 4,
    max_hops: int = 8,
) -> tuple[list[dict[str, object]], set[str]]:
    def is_shell_intermediary(node: str) -> bool:
        return (
            ctx.total_tx_count.get(node, 0) in (2, 3)
            and ctx.in_degree.get(node, 0) == 1
            and ctx.out_degree.get(node, 0) == 1
        )

    high_volume_threshold = _default_high_volume_threshold(ctx, min_length=min_length)
    suspicious_proxy_nodes = _build_suspicious_proxy_nodes(ctx)

    edge_times, edge_events = _build_edge_index(ctx)

    candidates: dict[frozenset[str], dict[str, object]] = {}
    seen_paths: set[tuple[str, ...]] = set()

    def register_candidate(record: dict[str, object]) -> None:
        members = tuple(str(account) for account in record["member_accounts"])
        members_set = frozenset(members)
        if members_set in candidates:
            return

        for existing_set in list(candidates):
            if members_set < existing_set:
                return
        for existing_set in list(candidates):
            if members_set > existing_set:
                candidates.pop(existing_set, None)

        candidates[members_set] = record

    def bounded_dfs(
        current: str,
        path: list[str],
        visited: set[str],
    ) -> None:
        if len(path) - 1 >= max_hops:
            return

        for nxt in ctx.adjacency.get(current, ()):
            if nxt in visited:
                continue

            new_path = path + [nxt]
            new_hops = len(new_path) - 1
            if new_hops > max_hops:
                continue

            # Intermediaries only: continue DFS through shell-qualified nodes.
            if is_shell_intermediary(nxt):
                bounded_dfs(nxt, new_path, visited | {nxt})
                continue

            if len(new_path) < min_length:
                continue

            path_key = tuple(new_path)
            if path_key in seen_paths:
                continue
            seen_paths.add(path_key)

            valid, hop_times, hop_amounts = _validate_path_events(
                path=path_key,
                edge_times=edge_times,
                edge_events=edge_events,
            )
            if not valid:
                continue

            source_node = path_key[0]
            destination_node = path_key[-1]
            chain_volume = float(sum(hop_amounts))

            linked_to_other_patterns = (
                source_node in suspicious_proxy_nodes
                or destination_node in suspicious_proxy_nodes
            )
            high_volume = chain_volume >= float(high_volume_threshold)
            rapid_forwarding = _is_rapid_forwarding(hop_times) and _has_short_holding(
                path_key, ctx
            )

            # Mandatory intent gate: topology-only linear chains are not shell laundering.
            if not (linked_to_other_patterns or high_volume or rapid_forwarding):
                continue

            members = tuple(sorted(set(path_key)))
            register_candidate(
                {
                    "member_accounts": list(members),
                    "pattern_type": "shell",
                    "path": path_key,
                    "chain_volume": float(f"{chain_volume:.2f}"),
                    "linked_to_other_patterns": linked_to_other_patterns,
                    "high_volume": high_volume,
                    "rapid_forwarding": rapid_forwarding,
                }
            )

    for source in ctx.accounts:
        if is_shell_intermediary(source):
            continue
        for nxt in ctx.adjacency.get(source, ()):
            if not is_shell_intermediary(nxt):
                continue
            bounded_dfs(nxt, [source, nxt], {source, nxt})

    # Deterministic non-overlap filter to avoid overlapping shell rings.
    sorted_candidates = sorted(
        candidates.values(),
        key=lambda item: (
            -len(tuple(item["path"])),
            -float(item.get("chain_volume", 0.0)),
            tuple(item["path"]),
        ),
    )

    selected: list[dict[str, object]] = []
    occupied_accounts: set[str] = set()
    shell_nodes: set[str] = set()

    for record in sorted_candidates:
        members = tuple(str(account) for account in record["member_accounts"])
        if any(account in occupied_accounts for account in members):
            continue
        selected.append(record)
        occupied_accounts.update(members)
        path = tuple(str(node) for node in record["path"])
        for node in path[1:-1]:
            shell_nodes.add(node)

    selected.sort(key=lambda item: tuple(item["member_accounts"]))
    return selected, shell_nodes
