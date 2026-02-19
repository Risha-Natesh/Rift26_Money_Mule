from __future__ import annotations

from bisect import bisect_left, bisect_right

import pandas as pd

from app.detection.preprocess import PreprocessResult


def _has_spike(
    times: list[pd.Timestamp],
    amounts: list[float],
    window_hours: float = 24.0,
    spike_ratio: float = 1.5,
) -> bool:
    if not times:
        return False
    window = pd.Timedelta(hours=window_hours)
    left = 0
    max_sum = 0.0
    total_sum = sum(amounts)
    rolling_sum = 0.0
    for right in range(len(times)):
        rolling_sum += float(amounts[right])
        while times[right] - times[left] > window:
            rolling_sum -= float(amounts[left])
            left += 1
        if rolling_sum > max_sum:
            max_sum = rolling_sum
    span_hours = max(1.0, (times[-1] - times[0]).total_seconds() / 3600.0)
    if span_hours <= window_hours:
        return True
    avg_sum = total_sum / max(1.0, span_hours / 24.0)
    return max_sum >= avg_sum * spike_ratio


def detect_smurfing(
    ctx: PreprocessResult,
    window_hours: int = 72,
) -> list[dict[str, object]]:
    smurf_rings: list[dict[str, object]] = []
    if not ctx.accounts:
        return smurf_rings

    small_amount_threshold = ctx.small_amount_threshold
    volume_threshold = small_amount_threshold * 10.0
    if small_amount_threshold <= 0.0 or volume_threshold <= 0.0:
        return smurf_rings

    window_delta = pd.Timedelta(hours=window_hours)
    dispersion_delta = pd.Timedelta(hours=24)

    for receiver in ctx.accounts:
        inbound = ctx.inbound_txs.get(receiver, [])
        if len(inbound) < 10:
            continue

        all_times = [item[0] for item in inbound]
        all_times.sort()
        if all_times and (all_times[-1] - all_times[0]).days >= 5:
            continue

        if ctx.median_holding_hours.get(receiver, 0.0) > 48.0:
            continue

        filtered = [(ts, sender, amt) for ts, sender, amt in inbound if amt <= small_amount_threshold]
        if len(filtered) < 10:
            continue

        times = [item[0] for item in filtered]
        senders = [item[1] for item in filtered]
        amounts = [item[2] for item in filtered]

        if not _has_spike(times, amounts):
            continue

        outbound = ctx.outbound_txs.get(receiver, [])
        out_times = [item[0] for item in outbound]
        out_amounts = [item[2] for item in outbound]
        out_prefix = [0.0]
        for amt in out_amounts:
            out_prefix.append(out_prefix[-1] + float(amt))

        def outbound_sum(start: pd.Timestamp, end: pd.Timestamp) -> tuple[float, int]:
            left = bisect_left(out_times, start)
            right = bisect_right(out_times, end)
            return out_prefix[right] - out_prefix[left], right - left

        left = 0
        sender_counts: dict[str, int] = {}
        current_sum = 0.0
        best_window = None

        for right in range(len(times)):
            sender = senders[right]
            sender_counts[sender] = sender_counts.get(sender, 0) + 1
            current_sum += float(amounts[right])

            while times[right] - times[left] > window_delta:
                left_sender = senders[left]
                sender_counts[left_sender] -= 1
                if sender_counts[left_sender] == 0:
                    sender_counts.pop(left_sender, None)
                current_sum -= float(amounts[left])
                left += 1

            unique_senders = len(sender_counts)
            if unique_senders < 10 or current_sum < volume_threshold:
                continue

            window_start = times[left]
            window_end = times[right]
            out_sum, _ = outbound_sum(window_end, window_end + dispersion_delta)
            if out_sum < 0.5 * current_sum:
                continue

            candidate = (
                float(current_sum),
                unique_senders,
                window_end,
                window_start,
                tuple(sorted(sender_counts)),
            )
            if best_window is None or candidate > best_window:
                best_window = candidate

        if best_window is None:
            continue

        _, _, window_end, window_start, sender_tuple = best_window
        members = tuple(sorted(set(sender_tuple) | {receiver}))
        smurf_rings.append(
            {
                "member_accounts": list(members),
                "pattern_type": "smurfing",
                "receiver": receiver,
                "senders": list(sender_tuple),
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
            }
        )

    smurf_rings.sort(key=lambda item: tuple(item["member_accounts"]))
    return smurf_rings
