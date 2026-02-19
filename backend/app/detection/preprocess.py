from __future__ import annotations

from dataclasses import dataclass
from collections import defaultdict

import numpy as np
import pandas as pd
import networkx as nx


@dataclass(frozen=True)
class PreprocessResult:
    accounts: tuple[str, ...]
    adjacency: dict[str, tuple[str, ...]]
    reverse_adjacency: dict[str, tuple[str, ...]]
    in_degree: dict[str, int]
    out_degree: dict[str, int]
    total_tx_count: dict[str, int]
    unique_counterparties: dict[str, int]
    inbound_volume: dict[str, float]
    outbound_volume: dict[str, float]
    median_holding_hours: dict[str, float]
    transaction_density: dict[str, float]
    sccs: tuple[tuple[str, ...], ...]
    scc_map: dict[str, int]
    tx_sorted: pd.DataFrame
    inbound_txs: dict[str, list[tuple[pd.Timestamp, str, float]]]
    outbound_txs: dict[str, list[tuple[pd.Timestamp, str, float]]]
    small_amount_threshold: float


def _rolling_max_events(
    timestamps: list[pd.Timestamp],
    window_hours: float,
) -> int:
    if not timestamps:
        return 0
    window_delta = pd.Timedelta(hours=window_hours)
    left = 0
    best = 0
    for right in range(len(timestamps)):
        while timestamps[right] - timestamps[left] > window_delta:
            left += 1
        best = max(best, right - left + 1)
    return best


def _median_or_default(values: list[float], default: float) -> float:
    if not values:
        return default
    values_sorted = sorted(values)
    mid = len(values_sorted) // 2
    if len(values_sorted) % 2 == 1:
        return float(values_sorted[mid])
    return float((values_sorted[mid - 1] + values_sorted[mid]) / 2.0)


def preprocess_transactions(df: pd.DataFrame) -> PreprocessResult:
    scoped = df.copy()
    scoped["sender_id"] = scoped["sender_id"].astype(str)
    scoped["receiver_id"] = scoped["receiver_id"].astype(str)
    scoped["amount"] = pd.to_numeric(scoped["amount"], errors="coerce")
    scoped["timestamp"] = pd.to_datetime(scoped["timestamp"], errors="coerce")

    accounts = tuple(sorted(set(scoped["sender_id"]).union(set(scoped["receiver_id"]))))

    edges = scoped[["sender_id", "receiver_id"]].drop_duplicates()
    adjacency_sets: dict[str, set[str]] = defaultdict(set)
    reverse_sets: dict[str, set[str]] = defaultdict(set)
    for sender, receiver in edges.itertuples(index=False, name=None):
        sender_id = str(sender)
        receiver_id = str(receiver)
        adjacency_sets[sender_id].add(receiver_id)
        reverse_sets[receiver_id].add(sender_id)
        adjacency_sets.setdefault(receiver_id, set())
        reverse_sets.setdefault(sender_id, set())

    adjacency = {
        node: tuple(sorted(neighbors))
        for node, neighbors in sorted(adjacency_sets.items(), key=lambda item: item[0])
    }
    reverse_adjacency = {
        node: tuple(sorted(neighbors))
        for node, neighbors in sorted(reverse_sets.items(), key=lambda item: item[0])
    }

    in_degree = {node: len(reverse_adjacency.get(node, ())) for node in accounts}
    out_degree = {node: len(adjacency.get(node, ())) for node in accounts}

    total_tx_count = (
        scoped["sender_id"].value_counts()
        .add(scoped["receiver_id"].value_counts(), fill_value=0)
        .astype(int)
        .to_dict()
    )

    inbound_volume = (
        scoped.groupby("receiver_id", sort=True)["amount"].sum().astype(float).to_dict()
    )
    outbound_volume = (
        scoped.groupby("sender_id", sort=True)["amount"].sum().astype(float).to_dict()
    )

    counterparties: dict[str, set[str]] = defaultdict(set)
    for sender, receiver in scoped[["sender_id", "receiver_id"]].itertuples(index=False, name=None):
        sender_id = str(sender)
        receiver_id = str(receiver)
        counterparties[sender_id].add(receiver_id)
        counterparties[receiver_id].add(sender_id)
    unique_counterparties = {
        account: len(counterparties.get(account, set())) for account in accounts
    }

    tx_sorted = scoped.sort_values(by=["timestamp", "transaction_id"], kind="mergesort")

    inbound_txs: dict[str, list[tuple[pd.Timestamp, str, float]]] = defaultdict(list)
    outbound_txs: dict[str, list[tuple[pd.Timestamp, str, float]]] = defaultdict(list)
    account_events: dict[str, list[pd.Timestamp]] = defaultdict(list)

    for sender, receiver, amount, timestamp in tx_sorted[
        ["sender_id", "receiver_id", "amount", "timestamp"]
    ].itertuples(index=False, name=None):
        sender_id = str(sender)
        receiver_id = str(receiver)
        ts = pd.Timestamp(timestamp)
        amt = float(amount)
        outbound_txs[sender_id].append((ts, receiver_id, amt))
        inbound_txs[receiver_id].append((ts, sender_id, amt))
        account_events[sender_id].append(ts)
        account_events[receiver_id].append(ts)

    median_holding_hours: dict[str, float] = {}
    transaction_density: dict[str, float] = {}

    for account in accounts:
        inbound_times = [event[0] for event in inbound_txs.get(account, [])]
        outbound_times = [event[0] for event in outbound_txs.get(account, [])]
        inbound_times.sort()
        outbound_times.sort()

        deltas: list[float] = []
        j = 0
        for inbound_time in inbound_times:
            while j < len(outbound_times) and outbound_times[j] <= inbound_time:
                j += 1
            if j < len(outbound_times):
                delta_hours = (outbound_times[j] - inbound_time).total_seconds() / 3600.0
                if delta_hours >= 0:
                    deltas.append(delta_hours)

        median_holding_hours[account] = _median_or_default(deltas, 1e9)

        events = sorted(account_events.get(account, []))
        max_events = _rolling_max_events(events, 24.0)
        transaction_density[account] = float(max_events)

    amounts = scoped["amount"].astype(float).to_numpy()
    if len(amounts) == 0:
        small_amount_threshold = 0.0
    else:
        small_amount_threshold = float(np.percentile(amounts, 25))

    graph = nx.DiGraph()
    graph.add_edges_from(edges.itertuples(index=False, name=None))
    sccs = []
    for component in nx.strongly_connected_components(graph):
        if not component:
            continue
        sccs.append(tuple(sorted(str(node) for node in component)))
    sccs_sorted = tuple(sorted(sccs, key=lambda comp: comp))
    scc_map = {}
    for index, component in enumerate(sccs_sorted):
        for node in component:
            scc_map[node] = index

    return PreprocessResult(
        accounts=accounts,
        adjacency=adjacency,
        reverse_adjacency=reverse_adjacency,
        in_degree={account: int(in_degree.get(account, 0)) for account in accounts},
        out_degree={account: int(out_degree.get(account, 0)) for account in accounts},
        total_tx_count={account: int(total_tx_count.get(account, 0)) for account in accounts},
        unique_counterparties=unique_counterparties,
        inbound_volume={account: float(inbound_volume.get(account, 0.0)) for account in accounts},
        outbound_volume={account: float(outbound_volume.get(account, 0.0)) for account in accounts},
        median_holding_hours=median_holding_hours,
        transaction_density=transaction_density,
        sccs=sccs_sorted,
        scc_map=scc_map,
        tx_sorted=tx_sorted,
        inbound_txs=inbound_txs,
        outbound_txs=outbound_txs,
        small_amount_threshold=small_amount_threshold,
    )
