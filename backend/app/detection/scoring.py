from __future__ import annotations

import math
from collections import defaultdict
from typing import Iterable

import numpy as np

from app.detection.preprocess import PreprocessResult


PATTERN_ORDER = [
    "cycle_length_3",
    "cycle_length_4",
    "cycle_length_5",
    "fan_in",
    "fan_out",
    "shell_chain",
    "merchant_laundering",
    "payroll_abuse",
    "high_velocity",
]

ROLE_MULTIPLIER = {
    "organizer": 1.2,
    "intermediary": 1.0,
    "destination": 0.9,
    "passive": 0.8,
}

ROLE_PRIORITY = {
    "organizer": 3,
    "intermediary": 2,
    "destination": 1,
    "passive": 0,
}

PATTERN_WEIGHTS = {
    "cycle": 0.75,
    "smurfing": 0.85,
    "shell": 0.70,
    "merchant": 0.90,
    "payroll": 0.80,
}


def _round2(value: float) -> float:
    return float(f"{value:.2f}")


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def ordered_patterns(patterns: set[str]) -> list[str]:
    return [pattern for pattern in PATTERN_ORDER if pattern in patterns]


def _normalize_counts(
    accounts: Iterable[str],
    counts: dict[str, int],
) -> dict[str, float]:
    if not counts:
        return {account: 0.0 for account in accounts}
    max_count = max(counts.values())
    if max_count <= 0:
        return {account: 0.0 for account in accounts}
    return {account: counts.get(account, 0) / max_count for account in accounts}


def _assign_role(
    role_map: dict[str, str],
    account: str,
    role: str,
) -> None:
    current = role_map.get(account, "passive")
    if ROLE_PRIORITY[role] > ROLE_PRIORITY[current]:
        role_map[account] = role


def _assign_roles(rings: list[dict[str, object]]) -> dict[str, str]:
    role_map: dict[str, str] = {}
    for ring in rings:
        pattern = str(ring.get("pattern_type", ""))
        members = [str(account) for account in ring.get("member_accounts", [])]

        if pattern == "cycle":
            for account in members:
                _assign_role(role_map, account, "intermediary")
            continue

        if pattern == "smurfing":
            receiver = str(ring.get("receiver", ""))
            if receiver:
                _assign_role(role_map, receiver, "organizer")
            for sender in ring.get("senders", []):
                _assign_role(role_map, str(sender), "passive")
            continue

        if pattern == "shell":
            path = ring.get("path")
            if isinstance(path, tuple) and len(path) >= 2:
                _assign_role(role_map, str(path[0]), "organizer")
                _assign_role(role_map, str(path[-1]), "destination")
                for intermediate in path[1:-1]:
                    _assign_role(role_map, str(intermediate), "intermediary")
            else:
                for account in members:
                    _assign_role(role_map, account, "intermediary")
            continue

        if pattern == "merchant":
            merchant = str(ring.get("merchant", ""))
            if merchant:
                _assign_role(role_map, merchant, "organizer")
            for sender in ring.get("inbound_suspicious_senders", []):
                _assign_role(role_map, str(sender), "passive")
            for receiver in ring.get("outbound_receivers", []):
                _assign_role(role_map, str(receiver), "destination")
            continue

        if pattern == "payroll":
            employer = str(ring.get("employer", ""))
            if employer:
                _assign_role(role_map, employer, "organizer")
            for employee in ring.get("employees", []):
                _assign_role(role_map, str(employee), "intermediary")
            continue

        for account in members:
            _assign_role(role_map, account, "passive")

    return role_map


def _velocity_factors(ctx: PreprocessResult) -> dict[str, float]:
    densities = [ctx.transaction_density.get(account, 0.0) for account in ctx.accounts]
    if not densities:
        return {account: 0.0 for account in ctx.accounts}
    p75 = float(np.percentile(densities, 75))
    p95 = float(np.percentile(densities, 95))
    if p95 <= p75:
        p95 = p75 + 1.0
    factors: dict[str, float] = {}
    for account in ctx.accounts:
        density = ctx.transaction_density.get(account, 0.0)
        if density <= p75:
            factors[account] = 0.0
        elif density >= p95:
            factors[account] = 1.0
        else:
            factors[account] = (density - p75) / (p95 - p75)
    return factors


def calculate_scores(
    ctx: PreprocessResult,
    rings: list[dict[str, object]],
    legitimate_merchants: set[str],
    legitimate_payroll: set[str],
) -> tuple[dict[str, float], dict[str, set[str]], dict[str, float]]:
    accounts = list(ctx.accounts)
    counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    cycle_labels: dict[str, set[str]] = defaultdict(set)
    fan_in_accounts: set[str] = set()
    fan_out_accounts: set[str] = set()
    shell_accounts: set[str] = set()
    merchant_accounts: set[str] = set()
    payroll_accounts: set[str] = set()

    for ring in rings:
        pattern = str(ring.get("pattern_type", ""))
        for account in ring.get("member_accounts", []):
            counts[pattern][str(account)] += 1
        if pattern == "cycle":
            cycle_length = int(ring.get("cycle_length", 0))
            if cycle_length in (3, 4, 5):
                label = f"cycle_length_{cycle_length}"
                for account in ring.get("member_accounts", []):
                    cycle_labels[str(account)].add(label)
        if pattern == "smurfing":
            receiver = str(ring.get("receiver", ""))
            if receiver:
                fan_in_accounts.add(receiver)
            for sender in ring.get("senders", []):
                fan_out_accounts.add(str(sender))
        if pattern == "shell":
            for account in ring.get("member_accounts", []):
                shell_accounts.add(str(account))
        if pattern == "merchant":
            for account in ring.get("member_accounts", []):
                merchant_accounts.add(str(account))
        if pattern == "payroll":
            for account in ring.get("member_accounts", []):
                payroll_accounts.add(str(account))

    cycle_scores = _normalize_counts(accounts, counts.get("cycle", {}))
    smurf_scores = _normalize_counts(accounts, counts.get("smurfing", {}))
    shell_scores = _normalize_counts(accounts, counts.get("shell", {}))
    merchant_scores = _normalize_counts(accounts, counts.get("merchant", {}))
    payroll_scores = _normalize_counts(accounts, counts.get("payroll", {}))
    velocity_scores = _velocity_factors(ctx)
    for account in accounts:
        if (
            cycle_scores.get(account, 0.0) == 0.0
            and smurf_scores.get(account, 0.0) == 0.0
            and shell_scores.get(account, 0.0) == 0.0
            and merchant_scores.get(account, 0.0) == 0.0
            and payroll_scores.get(account, 0.0) == 0.0
        ):
            velocity_scores[account] = 0.0

    role_map = _assign_roles(rings)

    raw_scores: dict[str, float] = {}
    for account in accounts:
        base = (
            0.30 * cycle_scores.get(account, 0.0)
            + 0.20 * smurf_scores.get(account, 0.0)
            + 0.15 * shell_scores.get(account, 0.0)
            + 0.15 * merchant_scores.get(account, 0.0)
            + 0.15 * payroll_scores.get(account, 0.0)
            + 0.05 * velocity_scores.get(account, 0.0)
        )
        role = role_map.get(account, "passive")
        adjusted = base * ROLE_MULTIPLIER.get(role, 1.0)
        if account in legitimate_merchants:
            adjusted *= 0.85
        if account in legitimate_payroll:
            adjusted *= 0.85
        raw_scores[account] = max(0.0, adjusted)

    if raw_scores:
        mu = sum(raw_scores.values()) / len(raw_scores)
    else:
        mu = 0.0

    scores: dict[str, float] = {}
    patterns: dict[str, set[str]] = {account: set() for account in accounts}

    for account in accounts:
        value = raw_scores.get(account, 0.0)
        score = 100.0 * (1.0 / (1.0 + math.exp(-6.0 * (value - mu))))
        scores[account] = _round2(_clamp(score, 0.0, 100.0))

        for label in sorted(cycle_labels.get(account, set())):
            patterns[account].add(label)
        if account in fan_in_accounts:
            patterns[account].add("fan_in")
        if account in fan_out_accounts:
            patterns[account].add("fan_out")
        if account in shell_accounts and shell_scores.get(account, 0.0) > 0:
            patterns[account].add("shell_chain")
        if account in merchant_accounts and merchant_scores.get(account, 0.0) > 0:
            patterns[account].add("merchant_laundering")
        if account in payroll_accounts and payroll_scores.get(account, 0.0) > 0:
            patterns[account].add("payroll_abuse")
        if velocity_scores.get(account, 0.0) > 0:
            patterns[account].add("high_velocity")

    return scores, patterns, velocity_scores


def compute_ring_risk(
    ring: dict[str, object],
    scores: dict[str, float],
    velocity_factors: dict[str, float],
    ctx: PreprocessResult,
) -> float:
    members = [str(account) for account in ring.get("member_accounts", [])]
    if not members:
        return 0.0
    avg_member_score = sum(scores.get(member, 0.0) for member in members) / len(members)

    pattern = str(ring.get("pattern_type", ""))
    pattern_weight = PATTERN_WEIGHTS.get(pattern, 0.65)

    member_set = set(members)
    edge_count = 0
    for sender in sorted(member_set):
        for receiver in ctx.adjacency.get(sender, ()):
            if receiver in member_set:
                edge_count += 1

    node_count = len(member_set)
    structural_complexity = math.log2(1.0 + node_count + edge_count)
    velocity_factor = sum(velocity_factors.get(member, 0.0) for member in members) / len(members)

    risk = (
        0.5 * avg_member_score
        + 0.25 * (pattern_weight * 100.0)
        + 0.15 * (structural_complexity * 10.0)
        + 0.10 * (velocity_factor * 100.0)
    )
    return _clamp(risk, 0.0, 100.0)
