from flask import Flask, request, send_file, jsonify
import csv
import os
import requests
from datetime import datetime, timezone
from threading import Thread
from typing import Any, Dict

app = Flask(__name__)

CSV_FILE = os.getenv("CSV_FILE", "journal_trades.csv")

GOOGLE_SCRIPT_URL = os.getenv("GOOGLE_SCRIPT_URL", "").strip()
GOOGLE_SCRIPT_SECRET = os.getenv("GOOGLE_SCRIPT_SECRET", "").strip()

if not GOOGLE_SCRIPT_URL:
    raise RuntimeError(
        "La variable d'environnement GOOGLE_SCRIPT_URL est absente."
    )

if not GOOGLE_SCRIPT_SECRET:
    raise RuntimeError(
        "La variable d'environnement GOOGLE_SCRIPT_SECRET est absente."
    )


CSV_COLUMNS = [
    "date",
    "symbol",
    "timeframe",
    "direction",
    "action",
    "mode",
    "setup",
    "score",
    "session",
    "entry",
    "sl",
    "tp",
    "rr",
    "htf_trend",
    "bos",
    "fvg",
    "sweep",
    "version",
    "fingerprint",
    "cis",
    "cis_score",
    "context_class",
    "learning_tag",
    "institutional_ai_label",
    "institutional_ai_score",
    "institutional_confidence_label",
    "institutional_confidence_score",
    "institutional_rank",
    "institutional_rank_score",
    "institutional_pattern",
    "institutional_pattern_quality",
    "institutional_outcome_key",
    "institutional_outcome_group",
    "context_scorecard",
    "context_scorecard_value",
    "tvs",
    "tvs_score",
    "penalty",
    "penalty_score",
    "exit_mode",
    "management_mode",
    "be_state",
    "thesis",
    "setup_id",
    "setup_family",
    "learning_bucket",
    "learning_bucket_score",
    "historical_context_group",
    "historical_context_quality",
    "edge_stability",
    "edge_stability_score",
    "dominant_scenario",
    "scenario_confidence",
    "bull_scenario",
    "bull_confidence",
    "bear_scenario",
    "bear_confidence",
    "narrative_phase",
    "narrative_score",
    "narrative_quality",
    "narrative_text",
    "alignment",
    "structure",
    "structure_strength",
    "trend_persistence",
    "choch",
    "choch_score",
    "consensus",
    "consensus_score",
    "cycle",
    "hierarchy_alignment",
    "sync_state",
    "context_score",
    "environment",
    "regime",
    "regime_quality",
    "regime_score",
    "v49_memory_score",
    "v49_memory_quality",
    "v49_memory_bias",
    "v49_memory_fingerprint",
    "v49_liquidity_memory_bias",
    "v49_liquidity_memory_score",
    "v49_fvg_bias",
    "v49_fvg_memory_score",
    "v49_htf_level_memory",
    "v49_htf_level_score",
    "resultat",
]


@app.get("/")
def health() -> Any:
    return jsonify(
        {
            "status": "ok",
            "service": "qros-railway-webhook",
        }
    ), 200


@app.post("/webhook")
def webhook() -> Any:
    data = request.get_json(silent=True)

    if not isinstance(data, dict):
        return jsonify(
            {
                "status": "error",
                "message": "Invalid JSON payload",
            }
        ), 400

    thread = Thread(
        target=process_webhook_background,
        args=(data.copy(),),
        daemon=True,
    )
    thread.start()

    return jsonify(
        {
            "status": "received",
            "message": "Webhook received by Railway",
        }
    ), 200


def process_webhook_background(data: Dict[str, Any]) -> None:
    try:
        trade = build_trade(data)

        append_trade_to_csv(trade)
        forward_trade_to_google_sheets(trade)

        print(
            "QROS WEBHOOK PROCESSED",
            {
                "symbol": trade.get("symbol"),
                "timeframe": trade.get("timeframe"),
                "action": trade.get("action"),
                "version": trade.get("version"),
                "resultat": trade.get("resultat"),
            },
            flush=True,
        )

    except Exception as error:
        print(
            "BACKGROUND ERROR =",
            repr(error),
            flush=True,
        )


def build_trade(data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "date": datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "symbol": data.get("symbol", ""),
        "timeframe": data.get("timeframe", ""),
        "direction": data.get("direction", ""),
        "action": data.get("action", ""),
        "mode": data.get("mode", ""),
        "setup": data.get("setup", ""),
        "score": data.get("score", 0),
        "session": data.get("session", ""),
        "entry": data.get("entry", 0),
        "sl": data.get("sl", 0),
        "tp": data.get("tp", 0),
        "rr": data.get("rr", 0),
        "htf_trend": data.get("htf_trend", ""),
        "bos": data.get("bos", False),
        "fvg": data.get("fvg", False),
        "sweep": data.get("sweep", False),

        "version": data.get("version", ""),
        "fingerprint": data.get("fingerprint", ""),
        "cis": data.get("cis", ""),
        "cis_score": data.get("cis_score", ""),
        "context_class": data.get("context_class", ""),
        "learning_tag": data.get("learning_tag", ""),
        "institutional_ai_label": data.get(
            "institutional_ai_label",
            "",
        ),
        "institutional_ai_score": data.get(
            "institutional_ai_score",
            "",
        ),
        "institutional_confidence_label": data.get(
            "institutional_confidence_label",
            "",
        ),
        "institutional_confidence_score": data.get(
            "institutional_confidence_score",
            "",
        ),
        "institutional_rank": data.get(
            "institutional_rank",
            "",
        ),
        "institutional_rank_score": data.get(
            "institutional_rank_score",
            "",
        ),
        "institutional_pattern": data.get(
            "institutional_pattern",
            "",
        ),
        "institutional_pattern_quality": data.get(
            "institutional_pattern_quality",
            "",
        ),
        "institutional_outcome_key": data.get(
            "institutional_outcome_key",
            "",
        ),
        "institutional_outcome_group": data.get(
            "institutional_outcome_group",
            "",
        ),
        "context_scorecard": data.get(
            "context_scorecard",
            "",
        ),
        "context_scorecard_value": data.get(
            "context_scorecard_value",
            "",
        ),

        "tvs": data.get("tvs", ""),
        "tvs_score": data.get("tvs_score", ""),
        "penalty": data.get("penalty", ""),
        "penalty_score": data.get("penalty_score", ""),
        "exit_mode": data.get("exit_mode", ""),
        "management_mode": data.get("management_mode", ""),
        "be_state": data.get("be_state", ""),
        "thesis": data.get("thesis", ""),

        "setup_id": data.get("setup_id", ""),
        "setup_family": data.get("setup_family", ""),
        "learning_bucket": data.get("learning_bucket", ""),
        "learning_bucket_score": data.get(
            "learning_bucket_score",
            "",
        ),
        "historical_context_group": data.get(
            "historical_context_group",
            "",
        ),
        "historical_context_quality": data.get(
            "historical_context_quality",
            "",
        ),
        "edge_stability": data.get("edge_stability", ""),
        "edge_stability_score": data.get(
            "edge_stability_score",
            "",
        ),

        "v49_memory_score": data.get("v49_memory_score"),
        "v49_memory_quality": data.get("v49_memory_quality"),
        "v49_memory_bias": data.get("v49_memory_bias"),
        "v49_memory_fingerprint": data.get(
            "v49_memory_fingerprint"
        ),
        "v49_liquidity_memory_bias": data.get(
            "v49_liquidity_memory_bias"
        ),
        "v49_liquidity_memory_score": data.get(
            "v49_liquidity_memory_score"
        ),
        "v49_fvg_bias": data.get("v49_fvg_bias"),
        "v49_fvg_memory_score": data.get(
            "v49_fvg_memory_score"
        ),
        "v49_htf_level_memory": data.get(
            "v49_htf_level_memory"
        ),
        "v49_htf_level_score": data.get(
            "v49_htf_level_score"
        ),

        "dominant_scenario": data.get(
            "dominant_scenario",
            "",
        ),
        "scenario_confidence": data.get(
            "scenario_confidence",
            "",
        ),
        "bull_scenario": data.get("bull_scenario", ""),
        "bull_confidence": data.get("bull_confidence", ""),
        "bear_scenario": data.get("bear_scenario", ""),
        "bear_confidence": data.get("bear_confidence", ""),

        "narrative_phase": data.get(
            "narrative_phase",
            "",
        ),
        "narrative_score": data.get(
            "narrative_score",
            "",
        ),
        "narrative_quality": data.get(
            "narrative_quality",
            "",
        ),
        "narrative_text": data.get("narrative_text", ""),
        "alignment": data.get("alignment", ""),
        "structure": data.get("structure", ""),
        "structure_strength": data.get(
            "structure_strength",
            "",
        ),
        "trend_persistence": data.get(
            "trend_persistence",
            "",
        ),
        "choch": data.get("choch", ""),
        "choch_score": data.get("choch_score", ""),
        "consensus": data.get("consensus", ""),
        "consensus_score": data.get(
            "consensus_score",
            "",
        ),
        "cycle": data.get("cycle", ""),
        "hierarchy_alignment": data.get(
            "hierarchy_alignment",
            "",
        ),
        "sync_state": data.get("sync_state", ""),
        "context_score": data.get("context_score", ""),
        "environment": data.get("environment", ""),

        "regime": data.get("regime", ""),
        "regime_quality": data.get("regime_quality", ""),
        "regime_score": data.get("regime_score", ""),

        "resultat": (
            data.get("result")
            or data.get("resultat")
            or "OPEN"
        ),
    }


def append_trade_to_csv(trade: Dict[str, Any]) -> None:
    file_exists = os.path.exists(CSV_FILE)
    file_is_empty = (
        not file_exists
        or os.path.getsize(CSV_FILE) == 0
    )

    with open(
        CSV_FILE,
        mode="a",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=CSV_COLUMNS,
            extrasaction="ignore",
        )

        if file_is_empty:
            writer.writeheader()

        writer.writerow(trade)


def forward_trade_to_google_sheets(
    trade: Dict[str, Any]
) -> None:
    google_payload = dict(trade)

    # Secret ajouté uniquement pour Apps Script.
    # Il n'est jamais écrit dans le CSV.
    google_payload["webhook_secret"] = (
        GOOGLE_SCRIPT_SECRET
    )

    response = requests.post(
        GOOGLE_SCRIPT_URL,
        json=google_payload,
        timeout=15,
    )

    response.raise_for_status()

    print(
        "GOOGLE SHEETS RESPONSE",
        {
            "status_code": response.status_code,
            "body": response.text[:500],
        },
        flush=True,
    )


@app.get("/download")
def download_csv() -> Any:
    if not os.path.exists(CSV_FILE):
        return jsonify(
            {
                "status": "error",
                "message": "CSV file not found",
            }
        ), 404

    return send_file(
        CSV_FILE,
        as_attachment=True,
    )


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))

    app.run(
        host="0.0.0.0",
        port=port,
    )
