from __future__ import annotations

from bisect import bisect_left

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


def _is_periodic(intervals: list[float]) -> bool:
    if len(intervals) < 2:
        return False
    intervals_sorted = sorted(intervals)
    median = intervals_sorted[len(intervals_sorted) // 2]
    if median <= 0:
        return False
    targets = [24.0, 168.0, 336.0, 720.0]
    return any(abs(median - target) <= (target * 0.2) for target in targets)


def _has_rapid_outflow(
    inbound_times: list[pd.Timestamp],
    outbound_times: list[pd.Timestamp],
    window_hours: float = 24.0,
) -> bool:
    if not inbound_times or not outbound_times:
        return False
    delta = pd.Timedelta(hours=window_hours)
    j = 0
    for inbound_time in inbound_times:
        while j < len(outbound_times) and outbound_times[j] < inbound_time:
            j += 1
        if j < len(outbound_times) and outbound_times[j] <= inbound_time + delta:
            return True
    return False


def _has_return_flow(
    inbound_times: list[pd.Timestamp],
    outbound: list[tuple[pd.Timestamp, str, float]],
    employer: str,
    window_hours: float = 72.0,
) -> bool:
    if not inbound_times or not outbound:
        return False
    delta = pd.Timedelta(hours=window_hours)
    out_times = [item[0] for item in outbound if item[1] == employer]
    if not out_times:
        return False
    out_times.sort()
    for inbound_time in inbound_times:
        idx = bisect_left(out_times, inbound_time)
        if idx < len(out_times) and out_times[idx] <= inbound_time + delta:
            return True
    return False


def detect_payroll_abuse(
    ctx: PreprocessResult,
    cycle_nodes: set[str],
    shell_nodes: set[str],
) -> tuple[list[dict[str, object]], set[str]]:
    rings: list[dict[str, object]] = []
    legitimate_payroll: set[str] = set()

    for employer in ctx.accounts:
        outbound = ctx.outbound_txs.get(employer, [])
        if len(outbound) < 5:
            continue

        receivers = [item[1] for item in outbound]
        unique_receivers = sorted(set(receivers))
        if len(unique_receivers) < 5:
            continue

        amounts = [float(item[2]) for item in outbound]
        if _coefficient_of_variation(amounts) > 0.15:
            continue

        out_times = [item[0] for item in outbound]
        out_times.sort()
        intervals = [
            (out_times[index] - out_times[index - 1]).total_seconds() / 3600.0
            for index in range(1, len(out_times))
        ]
        if not _is_periodic(intervals):
            continue

        rapid_employees: list[str] = []
        return_flow = False
        layering = False
        for employee in unique_receivers:
            inbound_times = [
                ts for ts, sender, amt in ctx.inbound_txs.get(employee, []) if sender == employer
            ]
            if not inbound_times:
                continue
            outbound_employee = ctx.outbound_txs.get(employee, [])
            outbound_times = [ts for ts, receiver, amt in outbound_employee]
            outbound_times.sort()
            if _has_rapid_outflow(inbound_times, outbound_times):
                rapid_employees.append(employee)
                if employee in cycle_nodes or employee in shell_nodes:
                    layering = True
                if any(receiver in shell_nodes or receiver in cycle_nodes for _, receiver, _ in outbound_employee):
                    layering = True
                if _has_return_flow(inbound_times, outbound_employee, employer):
                    return_flow = True

        if not unique_receivers:
            continue
        rapid_ratio = len(rapid_employees) / max(1, len(unique_receivers))
        if rapid_ratio < 0.40:
            legitimate_payroll.add(employer)
            continue

        linked = (
            employer in cycle_nodes
            or employer in shell_nodes
            or any(emp in cycle_nodes or emp in shell_nodes for emp in rapid_employees)
            or layering
        )
        if not linked:
            continue
        if not (return_flow or layering):
            continue

        members = tuple(sorted({employer} | set(rapid_employees)))
        rings.append(
            {
                "member_accounts": list(members),
                "pattern_type": "payroll",
                "employer": employer,
                "employees": sorted(rapid_employees),
            }
        )

    rings.sort(key=lambda item: tuple(item["member_accounts"]))
    return rings, legitimate_payroll
