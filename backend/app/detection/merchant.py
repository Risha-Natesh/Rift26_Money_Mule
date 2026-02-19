from __future__ import annotations

from collections import defaultdict
from bisect import bisect_left, bisect_right

import numpy as np
import pandas as pd

from app.detection.preprocess import PreprocessResult


def _coefficient_of_variation(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean_value = sum(values) / len(values)
    if mean_value == 0:
        return 0.0
    variance = sum((value - mean_value) ** 2 for value in values) / len(values)
    return (variance ** 0.5) / abs(mean_value)


def _rolling_max_sum(
    times: list[pd.Timestamp],
    amounts: list[float],
    window_hours: float,
) -> tuple[float, pd.Timestamp | None, pd.Timestamp | None]:
    if not times:
        return 0.0, None, None
    window = pd.Timedelta(hours=window_hours)
    left = 0
    rolling_sum = 0.0
    max_sum = 0.0
    best_start = None
    best_end = None
    for right in range(len(times)):
        rolling_sum += float(amounts[right])
        while times[right] - times[left] > window:
            rolling_sum -= float(amounts[left])
            left += 1
        if rolling_sum > max_sum:
            max_sum = rolling_sum
            best_start = times[left]
            best_end = times[right]
    return max_sum, best_start, best_end


def _is_steady_distribution(
    times: list[pd.Timestamp],
    amounts: list[float],
) -> bool:
    if len(times) < 6:
        return False
    daily: dict[pd.Timestamp, float] = defaultdict(float)
    for ts, amt in zip(times, amounts):
        daily[pd.Timestamp(ts.date())] += float(amt)
    if len(daily) < 3:
        return False
    values = list(daily.values())
    cv = _coefficient_of_variation(values)
    values_sorted = sorted(values)
    median = values_sorted[len(values_sorted) // 2]
    max_val = max(values_sorted)
    if median <= 0:
        return False
    return cv <= 0.20 and max_val <= median * 1.5


def detect_merchant_laundering(
    ctx: PreprocessResult,
    suspicious_accounts: set[str],
) -> tuple[list[dict[str, object]], set[str]]:
    rings: list[dict[str, object]] = []
    legitimate_merchants: set[str] = set()
    if not ctx.accounts:
        return rings, legitimate_merchants

    tx_counts = [ctx.total_tx_count.get(account, 0) for account in ctx.accounts]
    uniq_counts = [ctx.unique_counterparties.get(account, 0) for account in ctx.accounts]
    in_degrees = [ctx.in_degree.get(account, 0) for account in ctx.accounts]
    out_degrees = [ctx.out_degree.get(account, 0) for account in ctx.accounts]

    merchant_threshold = max(20, int(np.percentile(tx_counts, 75))) if tx_counts else 20
    unique_threshold = max(10, int(np.percentile(uniq_counts, 75))) if uniq_counts else 10
    in_degree_threshold = max(5, int(np.percentile(in_degrees, 75))) if in_degrees else 5
    out_degree_threshold = max(5, int(np.percentile(out_degrees, 75))) if out_degrees else 5

    for merchant in ctx.accounts:
        total_tx = ctx.total_tx_count.get(merchant, 0)
        unique_counterparties = ctx.unique_counterparties.get(merchant, 0)
        if total_tx < merchant_threshold:
            continue
        if unique_counterparties < unique_threshold:
            continue
        if ctx.in_degree.get(merchant, 0) < in_degree_threshold:
            continue
        if ctx.out_degree.get(merchant, 0) < out_degree_threshold:
            continue

        inbound = ctx.inbound_txs.get(merchant, [])
        outbound = ctx.outbound_txs.get(merchant, [])
        if not inbound or not outbound:
            continue

        in_times = [item[0] for item in inbound]
        in_senders = [item[1] for item in inbound]
        in_amounts = [float(item[2]) for item in inbound]
        total_inbound = sum(in_amounts)
        if total_inbound <= 0:
            continue

        suspicious_inbound = sum(
            amt for sender, amt in zip(in_senders, in_amounts) if sender in suspicious_accounts
        )
        suspicious_ratio = suspicious_inbound / total_inbound

        if ctx.median_holding_hours.get(merchant, 0.0) >= 48.0:
            if _is_steady_distribution([item[0] for item in outbound], [float(item[2]) for item in outbound]):
                legitimate_merchants.add(merchant)
            continue

        max_in_sum, window_start, window_end = _rolling_max_sum(in_times, in_amounts, 24.0)
        if max_in_sum <= 0 or window_end is None or window_start is None:
            continue

        out_times = [item[0] for item in outbound]
        out_receivers = [item[1] for item in outbound]
        out_amounts = [float(item[2]) for item in outbound]
        out_prefix = [0.0]
        for amt in out_amounts:
            out_prefix.append(out_prefix[-1] + float(amt))

        def outbound_sum(start: pd.Timestamp, end: pd.Timestamp) -> tuple[float, int]:
            left = bisect_left(out_times, start)
            right = bisect_right(out_times, end)
            return out_prefix[right] - out_prefix[left], right - left

        out_sum, out_count = outbound_sum(window_end, window_end + pd.Timedelta(hours=24))
        turnover_ratio = out_sum / max(1.0, max_in_sum)

        steady_outbound = _is_steady_distribution(out_times, out_amounts)
        if steady_outbound:
            legitimate_merchants.add(merchant)
            continue

        if suspicious_ratio < 0.30:
            continue
        if out_sum < 0.5 * max_in_sum:
            continue
        if turnover_ratio < 0.8:
            continue
        if out_count < max(3, int(0.2 * len(out_amounts))):
            continue

        inbound_suspicious_senders = sorted(
            {
                sender
                for ts, sender, amt in inbound
                if sender in suspicious_accounts and window_start <= ts <= window_end
            }
        )
        outbound_receivers = sorted(
            {
                receiver
                for ts, receiver, amt in outbound
                if window_end <= ts <= window_end + pd.Timedelta(hours=24)
            }
        )

        members = tuple(sorted({merchant} | set(inbound_suspicious_senders) | set(outbound_receivers)))
        rings.append(
            {
                "member_accounts": list(members),
                "pattern_type": "merchant",
                "merchant": merchant,
                "inbound_suspicious_senders": inbound_suspicious_senders,
                "outbound_receivers": outbound_receivers,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
            }
        )

    rings.sort(key=lambda item: tuple(item["member_accounts"]))
    return rings, legitimate_merchants
