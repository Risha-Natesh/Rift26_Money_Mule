from __future__ import annotations

import argparse
import csv
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from app.detection.cycles import detect_cycles
from app.detection.preprocess import preprocess_transactions
from app.detection.shell import detect_shell_paths
from app.detection.smurfing import detect_smurfing
from app.main import build_detection_result
from app.utils.validator import validate_and_parse_csv


def _build_df(records: list[tuple[str, str, str, float, str]]) -> pd.DataFrame:
    df = pd.DataFrame(
        records,
        columns=["transaction_id", "sender_id", "receiver_id", "amount", "timestamp"],
    )
    df["amount"] = pd.to_numeric(df["amount"], errors="raise")
    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        format="%Y-%m-%d %H:%M:%S",
        errors="raise",
    )
    return df


def _assert_no_null(value) -> None:
    if isinstance(value, dict):
        for inner_value in value.values():
            _assert_no_null(inner_value)
        return
    if isinstance(value, list):
        for item in value:
            _assert_no_null(item)
        return
    assert value is not None


def test_json_schema_and_key_order() -> None:
    records = [
        ("T001", "A", "B", 100.0, "2026-01-01 09:00:00"),
        ("T002", "B", "C", 100.0, "2026-01-01 09:10:00"),
        ("T003", "C", "A", 100.0, "2026-01-01 09:20:00"),
    ]
    df = _build_df(records)
    result = build_detection_result(df, processing_time_seconds=1.23)

    assert list(result.keys()) == ["suspicious_accounts", "fraud_rings", "summary"]
    assert list(result["summary"].keys()) == [
        "total_accounts_analyzed",
        "suspicious_accounts_flagged",
        "fraud_rings_detected",
        "processing_time_seconds",
    ]

    if result["suspicious_accounts"]:
        assert list(result["suspicious_accounts"][0].keys()) == [
            "account_id",
            "suspicion_score",
            "detected_patterns",
            "ring_id",
        ]

    if result["fraud_rings"]:
        assert list(result["fraud_rings"][0].keys()) == [
            "ring_id",
            "member_accounts",
            "pattern_type",
            "risk_score",
        ]

    _assert_no_null(result)


def test_cycle_detection_identifies_known_cycles() -> None:
    records = [
        ("C001", "A", "B", 10.0, "2026-01-01 00:00:00"),
        ("C002", "B", "C", 10.0, "2026-01-01 00:05:00"),
        ("C003", "C", "A", 10.0, "2026-01-01 00:10:00"),
        ("C004", "D", "E", 10.0, "2026-01-02 00:00:00"),
        ("C005", "E", "F", 10.0, "2026-01-02 00:05:00"),
        ("C006", "F", "G", 10.0, "2026-01-02 00:10:00"),
        ("C007", "G", "D", 10.0, "2026-01-02 00:15:00"),
    ]
    df = _build_df(records)
    ctx = preprocess_transactions(df)
    cycles, account_patterns = detect_cycles(ctx)

    cycle_members = {tuple(entry["member_accounts"]) for entry in cycles}
    assert ("A", "B", "C") in cycle_members
    assert ("D", "E", "F", "G") in cycle_members
    assert "cycle" in account_patterns["A"]
    assert "cycle" in account_patterns["D"]


def test_smurfing_fan_in_out_detection() -> None:
    base = datetime(2026, 1, 1, 0, 0, 0)
    records: list[tuple[str, str, str, float, str]] = []

    for index in range(10):
        records.append(
            (
                f"FI{index:03d}",
                f"S{index:02d}",
                "HUB",
                10.0,
                (base + timedelta(hours=index * 2)).strftime("%Y-%m-%d %H:%M:%S"),
            )
        )

    for index in range(10):
        records.append(
            (
                f"FO{index:03d}",
                "HUB",
                f"R{index:02d}",
                10.0,
                (base + timedelta(hours=15 + index)).strftime("%Y-%m-%d %H:%M:%S"),
            )
        )

    df = _build_df(records)
    ctx = preprocess_transactions(df)
    rings = detect_smurfing(ctx)

    assert any(ring["pattern_type"] == "smurfing" and ring.get("receiver") == "HUB" for ring in rings)


def test_smurfing_includes_72h_boundary() -> None:
    base = datetime(2026, 1, 1, 0, 0, 0)
    records: list[tuple[str, str, str, float, str]] = []
    for index in range(8):
        # Cluster within 24h to create a spike, while total span hits 72h.
        hour_offset = 2 * index
        records.append(
            (
                f"BND{index:03d}",
                f"S{index:02d}",
                "BOUNDARY_HUB",
                10.0,
                (base + timedelta(hours=hour_offset)).strftime("%Y-%m-%d %H:%M:%S"),
            )
        )
    for index in range(8, 10):
        hour_offset = 72 - (10 - index)
        records.append(
            (
                f"BND{index:03d}",
                f"S{index:02d}",
                "BOUNDARY_HUB",
                10.0,
                (base + timedelta(hours=hour_offset)).strftime("%Y-%m-%d %H:%M:%S"),
            )
        )
    for index in range(6):
        records.append(
            (
                f"BND_OUT{index:03d}",
                "BOUNDARY_HUB",
                f"R{index:02d}",
                10.0,
                (base + timedelta(hours=73 + index)).strftime("%Y-%m-%d %H:%M:%S"),
            )
        )
    df = _build_df(records)
    ctx = preprocess_transactions(df)
    rings = detect_smurfing(ctx, window_hours=72)
    assert any(ring.get("receiver") == "BOUNDARY_HUB" for ring in rings)


def test_shell_detection_identifies_layered_paths() -> None:
    records = [
        ("S001", "SRC", "MID1", 1000.0, "2026-01-01 01:00:00"),
        ("S002", "MID1", "MID2", 980.0, "2026-01-01 01:10:00"),
        ("S003", "MID2", "DST", 970.0, "2026-01-01 01:20:00"),
    ]
    df = _build_df(records)
    ctx = preprocess_transactions(df)
    paths, shell_nodes = detect_shell_paths(ctx)

    assert any(record["path"] == ("SRC", "MID1", "MID2", "DST") for record in paths)
    assert "MID1" in shell_nodes
    assert "MID2" in shell_nodes


def test_shell_detection_rejects_topology_only_linear_chain() -> None:
    records = [
        ("L001", "N1", "N2", 100.0, "2026-01-01 01:00:00"),
        ("L002", "N2", "N3", 130.0, "2026-01-01 01:20:00"),
        ("L003", "N3", "N4", 95.0, "2026-01-01 01:40:00"),
        ("L004", "N4", "N5", 150.0, "2026-01-01 02:00:00"),
    ]
    df = _build_df(records)
    ctx = preprocess_transactions(df)
    paths, shell_nodes = detect_shell_paths(ctx)

    assert paths == []
    assert shell_nodes == set()


def test_trap_shell_not_combinatorial_ring_explosion() -> None:
    csv_path = ROOT_DIR / "sample_data" / "trap_shell.csv"
    df = validate_and_parse_csv(csv_path.read_bytes())
    result = build_detection_result(df, processing_time_seconds=0.0)

    assert result["summary"]["total_accounts_analyzed"] == 14
    assert result["summary"]["fraud_rings_detected"] <= 4
    flagged_ids = {row["account_id"] for row in result["suspicious_accounts"]}
    assert "NORMAL_1" not in flagged_ids
    assert "NORMAL_2" not in flagged_ids
    assert "NORMAL_3" not in flagged_ids
    assert "NORMAL_4" not in flagged_ids


def test_validator_accepts_quoted_single_field_lines() -> None:
    payload = (
        '"transaction_id,sender_id,receiver_id,amount,timestamp"\n'
        '"Q001,A,B,100.0,2026-01-01 00:00:00"\n'
        '"Q002,B,C,99.5,2026-01-01 00:10:00"\n'
        '"Q003,C,A,98.0,2026-01-01 00:20:00"\n'
    ).encode("utf-8")
    df = validate_and_parse_csv(payload)
    assert len(df) == 3
    assert list(df.columns) == [
        "transaction_id",
        "sender_id",
        "receiver_id",
        "amount",
        "timestamp",
    ]


def generate_performance_csv(
    output_path: Path,
    rows: int = 10_000,
    account_count: int = 1_200,
) -> Path:
    start = datetime(2026, 1, 1, 0, 0, 0)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["transaction_id", "sender_id", "receiver_id", "amount", "timestamp"])

        for index in range(rows):
            sender = f"ACC_{index % account_count:04d}"
            receiver_index = (index * 17 + 31) % account_count
            receiver = f"ACC_{receiver_index:04d}"
            if receiver == sender:
                receiver = f"ACC_{(receiver_index + 1) % account_count:04d}"

            amount = ((index % 250) + 1) * 3.75
            timestamp = start + timedelta(minutes=index % (14 * 24 * 60))
            writer.writerow(
                [
                    f"TX_{index:06d}",
                    sender,
                    receiver,
                    f"{amount:.2f}",
                    timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                ]
            )

    return output_path


def test_performance_generator_creates_valid_csv(tmp_path: Path) -> None:
    csv_path = generate_performance_csv(tmp_path / "perf.csv", rows=2000, account_count=300)
    df = validate_and_parse_csv(csv_path.read_bytes())
    assert len(df) == 2000


def run_performance(
    rows: int = 10_000,
    account_count: int = 1_200,
    output: Path = Path("sample_data/performance_10k.csv"),
    assert_under: float | None = 30.0,
) -> float:
    csv_path = generate_performance_csv(output, rows=rows, account_count=account_count)
    df = validate_and_parse_csv(csv_path.read_bytes())

    started = time.perf_counter()
    _ = build_detection_result(df, processing_time_seconds=0.0)
    elapsed = time.perf_counter() - started

    print(f"CSV generated: {csv_path}")
    print(f"Rows: {rows}")
    print(f"Detection runtime: {elapsed:.2f}s")

    if assert_under is not None and elapsed > assert_under:
        raise SystemExit(
            f"Performance check failed: {elapsed:.2f}s > {assert_under:.2f}s"
        )

    return elapsed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Performance CSV generator + detector benchmark")
    parser.add_argument("--perf", action="store_true", help="Run performance generation and timing")
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--account-count", type=int, default=1_200)
    parser.add_argument("--output", type=Path, default=Path("sample_data/performance_10k.csv"))
    parser.add_argument("--assert-under", type=float, default=30.0)
    parser.add_argument(
        "--no-assert",
        action="store_true",
        help="Do not enforce runtime threshold",
    )
    args = parser.parse_args()

    if args.perf:
        limit = None if args.no_assert else args.assert_under
        run_performance(
            rows=args.rows,
            account_count=args.account_count,
            output=args.output,
            assert_under=limit,
        )
    else:
        print("Run with --perf to generate a performance dataset and benchmark detection.")
