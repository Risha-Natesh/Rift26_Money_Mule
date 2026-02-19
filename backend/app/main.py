from __future__ import annotations

import json
import logging
import os

import pandas as pd
from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response

from app.detection.cycles import detect_cycles
from app.detection.merchant import detect_merchant_laundering
from app.detection.payroll import detect_payroll_abuse
from app.detection.preprocess import preprocess_transactions
from app.detection.scoring import calculate_scores, compute_ring_risk, ordered_patterns
from app.detection.shell import detect_shell_paths
from app.detection.smurfing import detect_smurfing
from app.utils.perf import PerfTimer
from app.utils.validator import CSVValidationError, validate_and_parse_csv, validate_upload_metadata

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("rift.money_muling")
APP_ENV = os.getenv("APP_ENV", "development").strip().lower()


def _round2(value: float) -> float:
    return float(f"{value:.2f}")


def _sanitize_patterns(patterns: list[object]) -> list[str]:
    return [str(pattern) for pattern in patterns]


def _dedupe_rings(ring_candidates: list[dict[str, object]]) -> list[dict[str, object]]:
    registry: dict[frozenset[str], dict[str, object]] = {}

    for ring in ring_candidates:
        members = tuple(sorted(str(account) for account in ring.get("member_accounts", [])))
        if not members:
            continue
        ring["member_accounts"] = list(members)
        members_set = frozenset(members)

        if members_set in registry:
            continue

        discard = False
        to_remove: list[frozenset[str]] = []
        for existing_set in sorted(registry, key=lambda item: tuple(sorted(item))):
            if members_set < existing_set:
                discard = True
                break
            if members_set > existing_set:
                to_remove.append(existing_set)
        if discard:
            continue
        for existing_set in to_remove:
            registry.pop(existing_set, None)
        registry[members_set] = ring

    rings: list[dict[str, object]] = []
    for index, members_set in enumerate(
        sorted(registry, key=lambda item: tuple(sorted(item))), start=1
    ):
        ring = registry[members_set]
        ring["ring_id"] = f"RING_{index:03d}"
        rings.append(ring)

    return rings


def _primary_ring_map(rings: list[dict[str, object]]) -> dict[str, str]:
    account_to_ring: dict[str, str] = {}
    for ring in rings:
        ring_id = str(ring["ring_id"])
        for account in ring["member_accounts"]:
            account_id = str(account)
            if account_id not in account_to_ring:
                account_to_ring[account_id] = ring_id
    return account_to_ring


def build_detection_result(
    df: pd.DataFrame,
    processing_time_seconds: float = 0.0,
) -> dict[str, object]:
    suspicious_min_score = float(os.getenv("SUSPICIOUS_MIN_SCORE", "20"))

    ctx = preprocess_transactions(df)

    cycles, _ = detect_cycles(ctx)
    smurf_rings = detect_smurfing(ctx)
    cycle_nodes = {
        str(account)
        for cycle in cycles
        for account in cycle.get("member_accounts", [])
    }
    smurf_nodes = {
        str(account)
        for ring in smurf_rings
        for account in ring.get("member_accounts", [])
    }
    shell_rings, shell_nodes = detect_shell_paths(ctx)
    suspicious_seed = set().union(cycle_nodes, smurf_nodes, shell_nodes)

    merchant_rings, legitimate_merchants = detect_merchant_laundering(ctx, suspicious_seed)
    payroll_rings, legitimate_payroll = detect_payroll_abuse(ctx, cycle_nodes, shell_nodes)

    ring_candidates = (
        list(cycles) + list(smurf_rings) + list(shell_rings) + list(merchant_rings) + list(payroll_rings)
    )
    rings = _dedupe_rings(ring_candidates)

    scores, pattern_map, velocity_factors = calculate_scores(
        ctx=ctx,
        rings=rings,
        legitimate_merchants=legitimate_merchants,
        legitimate_payroll=legitimate_payroll,
    )
    account_ring = _primary_ring_map(rings)

    suspicious_accounts: list[dict[str, object]] = []
    for account in sorted(account_ring):
        score = _round2(float(scores.get(account, 0.0)))
        detected = pattern_map.get(account, set())
        if score < suspicious_min_score or not detected:
            continue

        detected_patterns = _sanitize_patterns(ordered_patterns(detected))
        suspicious_accounts.append(
            {
                "account_id": str(account),
                "suspicion_score": float(_round2(min(100.0, max(0.0, score)))),
                "detected_patterns": detected_patterns,
                "ring_id": str(account_ring[account]),
            }
        )

    suspicious_accounts.sort(
        key=lambda item: (-item["suspicion_score"], item["account_id"])
    )

    fraud_rings: list[dict[str, object]] = []
    for ring in sorted(rings, key=lambda item: item["ring_id"]):
        members = [str(account) for account in ring["member_accounts"]]
        risk_score = compute_ring_risk(
            ring=ring,
            scores=scores,
            velocity_factors=velocity_factors,
            ctx=ctx,
        )
        fraud_rings.append(
            {
                "ring_id": str(ring["ring_id"]),
                "member_accounts": members,
                "pattern_type": str(ring["pattern_type"]),
                "risk_score": _round2(risk_score),
            }
        )

    result = {
        "suspicious_accounts": suspicious_accounts,
        "fraud_rings": fraud_rings,
        "summary": {
            "total_accounts_analyzed": int(len(ctx.accounts)),
            "suspicious_accounts_flagged": int(len(suspicious_accounts)),
            "fraud_rings_detected": int(len(fraud_rings)),
            "processing_time_seconds": _round2(processing_time_seconds),
        },
    }
    return result


origins_setting = os.getenv("FRONTEND_ORIGINS", "*")
allowed_origins = (
    ["*"]
    if origins_setting.strip() == "*"
    else [origin.strip() for origin in origins_setting.split(",") if origin.strip()]
)

app = FastAPI(title="RIFT 2026 Money Muling Detection API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.state.last_result_json = ""


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/analyze")
async def analyze_csv(file: UploadFile = File(...)) -> JSONResponse:
    try:
        payload = await file.read()
        validate_upload_metadata(file.filename, file.content_type, payload)
        df = validate_and_parse_csv(payload)

        with PerfTimer() as timer:
            result = build_detection_result(df, processing_time_seconds=0.0)

        result["summary"]["processing_time_seconds"] = _round2(timer.elapsed_seconds)
        serialized = json.dumps(
            result,
            indent=2,
            ensure_ascii=False,
        )
        app.state.last_result_json = serialized
        return JSONResponse(content=result)

    except CSVValidationError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})

    except Exception as exc:  # pragma: no cover
        logger.exception("unexpected_error")
        if APP_ENV == "production":
            return JSONResponse(status_code=500, content={"error": "Internal server error"})
        return JSONResponse(
            status_code=500,
            content={"error": f"Internal server error: {exc}"},
        )


@app.get("/results/download")
async def download_result() -> Response:
    last_result_json = getattr(app.state, "last_result_json", "")
    if not last_result_json:
        return JSONResponse(
            status_code=404,
            content={"error": "No analysis result found. Upload and analyze a CSV first."},
        )

    headers = {"Content-Disposition": "attachment; filename=detection_results.json"}
    return Response(content=last_result_json, media_type="application/json", headers=headers)
