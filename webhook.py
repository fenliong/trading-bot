from flask import Flask, request, send_file, jsonify
import csv
import os
import requests
from datetime import datetime
from threading import Thread

app = Flask(__name__)

fichier = "journal_trades.csv"

GOOGLE_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbyVUSrnvHzkFTyuMqZfjEvXbOe2_bkFRsCYbdtfXuR3MZgGJePgh5vF9-eqeHnCeDCq/exec"

GOOGLE_SCRIPT_SECRET = os.getenv("GOOGLE_SCRIPT_SECRET", "").strip()

# ============================================================
# QROS STEP 45B.1
# Railway Payload Contract Guard
# Version: 0.1.0
# Default mode: SHADOW
# ============================================================

QROS_PAYLOAD_GUARD_MODE = os.getenv(
    "QROS_PAYLOAD_GUARD_MODE",
    "SHADOW"
).strip().upper()

QROS_SUPPORTED_VERSION = "QROS_V49.0.1"

QROS_ALLOWED_RESULTS = {
    "OPEN",
    "WIN",
    "LOSS",
    "BE"
}

QROS_ALLOWED_DIRECTIONS = {
    "LONG",
    "SHORT"
}

QROS_ALLOWED_ACTIONS = {
    "buy",
    "sell"
}


def _qros_non_empty_string(value):
    return (
        isinstance(value, str)
        and bool(value.strip())
    )


def _qros_number_like(value):
    """
    Accept real numeric values and numeric strings.

    This is intentional because the current Pine V49 contract
    sends some price fields as JSON strings.
    """

    if isinstance(value, bool):
        return False

    try:
        number = float(value)
    except (TypeError, ValueError):
        return False

    return (
        number == number
        and number not in (
            float("inf"),
            float("-inf")
        )
    )


def validate_qros_v49_payload(data):
    """
    STEP 45B contract validation.

    Validation only.
    No mutation.
    No persistence.
    No Google call.

    Returns:
        {
            "valid": bool,
            "errors": [...],
            "warnings": [...]
        }
    """

    errors = []
    warnings = []

    # --------------------------------------------------------
    # 1. ROOT CONTRACT
    # --------------------------------------------------------

    if not isinstance(data, dict):
        return {
            "valid": False,
            "errors": [
                "PAYLOAD_NOT_OBJECT"
            ],
            "warnings": []
        }

    # --------------------------------------------------------
    # 2. REQUIRED IDENTITY
    # --------------------------------------------------------

    trade_id = data.get("trade_id")

    if not _qros_non_empty_string(trade_id):
        errors.append(
            "TRADE_ID_MISSING_OR_EMPTY"
        )

    elif not trade_id.startswith("QTR1_"):
        warnings.append(
            "TRADE_ID_NON_STANDARD_PREFIX"
        )

    version = data.get("version")

    if version != QROS_SUPPORTED_VERSION:
        errors.append(
            "UNSUPPORTED_VERSION"
        )

    # --------------------------------------------------------
    # 3. ASSET CONTRACT
    # --------------------------------------------------------

    symbol = data.get("symbol")
    asset = data.get("asset")

    if not _qros_non_empty_string(symbol):
        errors.append(
            "SYMBOL_MISSING_OR_EMPTY"
        )

    if asset is not None:

        if not _qros_non_empty_string(asset):
            errors.append(
                "ASSET_INVALID"
            )

        elif (
            _qros_non_empty_string(symbol)
            and asset != symbol
        ):
            errors.append(
                "SYMBOL_ASSET_MISMATCH"
            )

    # --------------------------------------------------------
    # 4. TIMEFRAME CONTRACT
    # --------------------------------------------------------

    timeframe = data.get("timeframe")
    tf = data.get("tf")

    if not _qros_non_empty_string(timeframe):
        errors.append(
            "TIMEFRAME_MISSING_OR_EMPTY"
        )

    if tf is not None:

        if not _qros_non_empty_string(tf):
            errors.append(
                "TF_INVALID"
            )

        elif (
            _qros_non_empty_string(timeframe)
            and tf != timeframe
        ):
            errors.append(
                "TIMEFRAME_TF_MISMATCH"
            )

    # --------------------------------------------------------
    # 5. DIRECTION / ACTION CONTRACT
    # --------------------------------------------------------

    direction = data.get("direction")

    if direction not in QROS_ALLOWED_DIRECTIONS:
        errors.append(
            "INVALID_DIRECTION"
        )

    action = data.get("action")

    if action not in QROS_ALLOWED_ACTIONS:
        errors.append(
            "INVALID_ACTION"
        )

    if (
        direction == "LONG"
        and action != "buy"
    ):
        errors.append(
            "LONG_ACTION_MISMATCH"
        )

    if (
        direction == "SHORT"
        and action != "sell"
    ):
        errors.append(
            "SHORT_ACTION_MISMATCH"
        )

    # --------------------------------------------------------
    # 6. RESULT CONTRACT
    # --------------------------------------------------------

    result = data.get("result")

    if result not in QROS_ALLOWED_RESULTS:
        errors.append(
            "INVALID_RESULT"
        )

    # --------------------------------------------------------
    # 7. REQUIRED TRADE NUMERICS
    # --------------------------------------------------------

    required_numeric_fields = (
        "entry",
        "sl",
        "tp",
        "rr",
        "score"
    )

    for field_name in required_numeric_fields:

        value = data.get(field_name)

        if not _qros_number_like(value):
            errors.append(
                "INVALID_NUMERIC_" +
                field_name.upper()
            )

    # --------------------------------------------------------
    # 8. BASIC PRICE SANITY
    # --------------------------------------------------------

    if _qros_number_like(data.get("entry")):

        if float(data["entry"]) <= 0:
            errors.append(
                "ENTRY_NOT_POSITIVE"
            )

    if _qros_number_like(data.get("sl")):

        if float(data["sl"]) <= 0:
            errors.append(
                "SL_NOT_POSITIVE"
            )

    if _qros_number_like(data.get("tp")):

        if float(data["tp"]) <= 0:
            errors.append(
                "TP_NOT_POSITIVE"
            )

    if _qros_number_like(data.get("rr")):

        if float(data["rr"]) <= 0:
            errors.append(
                "RR_NOT_POSITIVE"
            )

    # --------------------------------------------------------
    # 9. ENTRY GEOMETRY
    #
    # LONG:
    # SL < ENTRY < TP
    #
    # SHORT:
    # TP < ENTRY < SL
    # --------------------------------------------------------

    if all(
        _qros_number_like(data.get(name))
        for name in (
            "entry",
            "sl",
            "tp"
        )
    ):

        entry = float(
            data["entry"]
        )

        sl = float(
            data["sl"]
        )

        tp = float(
            data["tp"]
        )

        if direction == "LONG":

            if not (
                sl < entry < tp
            ):
                errors.append(
                    "INVALID_LONG_PRICE_GEOMETRY"
                )

        elif direction == "SHORT":

            if not (
                tp < entry < sl
            ):
                errors.append(
                    "INVALID_SHORT_PRICE_GEOMETRY"
                )

    # --------------------------------------------------------
    # 10. LEARNING IDENTITY SUPPORT
    # --------------------------------------------------------

    fingerprint = data.get(
        "fingerprint"
    )

    if not _qros_non_empty_string(
        fingerprint
    ):
        warnings.append(
            "FINGERPRINT_MISSING_OR_EMPTY"
        )

    setup = data.get(
        "setup"
    )

    if not _qros_non_empty_string(
        setup
    ):
        warnings.append(
            "SETUP_MISSING_OR_EMPTY"
        )

    # --------------------------------------------------------
    # FINAL
    # --------------------------------------------------------

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings
    }
@app.route("/webhook", methods=["POST"])
def webhook():

    # ========================================================
    # STEP 45B.1 — SAFE JSON PARSING
    # ========================================================

    data = request.get_json(
        silent=True
    )

    if not isinstance(data, dict):

        print(
            "QROS_PAYLOAD_GUARD "
            "VALID=false "
            "ERRORS=['INVALID_JSON_OR_ROOT']",
            flush=True
        )

        return jsonify({
            "status": "rejected",
            "guard": "QROS_STEP45B1",
            "reason": "INVALID_JSON_OR_ROOT"
        }), 400

    # ========================================================
    # STEP 45B.1 — CONTRACT VALIDATION
    # ========================================================

    validation = validate_qros_v49_payload(
        data
    )

    print(
        "QROS_PAYLOAD_GUARD",
        "MODE=" + QROS_PAYLOAD_GUARD_MODE,
        "VALID=" + str(
            validation["valid"]
        ),
        "TRADE_ID=" + str(
            data.get(
                "trade_id",
                ""
            )
        ),
        "RESULT=" + str(
            data.get(
                "result",
                ""
            )
        ),
        "ERRORS=" + str(
            validation["errors"]
        ),
        "WARNINGS=" + str(
            validation["warnings"]
        ),
        flush=True
    )

    # ========================================================
    # ENFORCE MODE
    #
    # IMPORTANT:
    # During STEP 45B.1 we remain SHADOW.
    # This branch will therefore NOT run yet.
    # ========================================================

    if (
        QROS_PAYLOAD_GUARD_MODE
        == "ENFORCE"
        and not validation["valid"]
    ):

        return jsonify({
            "status": "rejected",
            "guard": "QROS_STEP45B1",
            "errors":
                validation["errors"],
            "warnings":
                validation["warnings"]
        }), 422

    # ========================================================
    # EXISTING DELIVERY PATH
    #
    # Preserved unchanged for STEP 45B.
    # ========================================================

    Thread(
        target=process_webhook_background,
        args=(data,),
        daemon=True
    ).start()

    return jsonify({
        "status": "received",
        "message":
            "Webhook received by Railway",

        "payload_guard": {
            "mode":
                QROS_PAYLOAD_GUARD_MODE,

            "valid":
                validation["valid"],

            "errors":
                validation["errors"],

            "warnings":
                validation["warnings"]
        }
    }), 200

def process_webhook_background(data):
    try:
        if not GOOGLE_SCRIPT_SECRET:
            raise RuntimeError(
                "GOOGLE_SCRIPT_SECRET manquant dans Railway Variables"
            )
        
        trade = {
            "trade_id": data.get("trade_id", ""),            
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
            "institutional_ai_label": data.get("institutional_ai_label", ""),
            "institutional_ai_score": data.get("institutional_ai_score", ""),
            "institutional_confidence_label": data.get("institutional_confidence_label", ""),
            "institutional_confidence_score": data.get("institutional_confidence_score", ""),
            "institutional_rank": data.get("institutional_rank", ""),
            "institutional_rank_score": data.get("institutional_rank_score", ""),
            "institutional_pattern": data.get("institutional_pattern", ""),
            "institutional_pattern_quality": data.get("institutional_pattern_quality", ""),
            "institutional_outcome_key": data.get("institutional_outcome_key", ""),
            "institutional_outcome_group": data.get("institutional_outcome_group", ""),
            "context_scorecard": data.get("context_scorecard", ""),
            "context_scorecard_value": data.get("context_scorecard_value", ""),

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
            "learning_bucket_score": data.get("learning_bucket_score", ""),
            "historical_context_group": data.get("historical_context_group", ""),
            "historical_context_quality": data.get("historical_context_quality", ""),
            "edge_stability": data.get("edge_stability", ""),
            "edge_stability_score": data.get("edge_stability_score", ""),

            "v49_memory_score": data.get("v49_memory_score"),
            "v49_memory_quality": data.get("v49_memory_quality"),
            "v49_memory_bias": data.get("v49_memory_bias"),
            "v49_memory_fingerprint": data.get("v49_memory_fingerprint"),
            "v49_liquidity_memory_bias": data.get("v49_liquidity_memory_bias"),
            "v49_liquidity_memory_score": data.get("v49_liquidity_memory_score"),
            "v49_fvg_bias": data.get("v49_fvg_bias"),
            "v49_fvg_memory_score": data.get("v49_fvg_memory_score"),
            "v49_htf_level_memory": data.get("v49_htf_level_memory"),
            "v49_htf_level_score": data.get("v49_htf_level_score"),

            "dominant_scenario": data.get("dominant_scenario", ""),
            "scenario_confidence": data.get("scenario_confidence", ""),
            "bull_scenario": data.get("bull_scenario", ""),
            "bull_confidence": data.get("bull_confidence", ""),
            "bear_scenario": data.get("bear_scenario", ""),
            "bear_confidence": data.get("bear_confidence", ""),

            "narrative_phase": data.get("narrative_phase", ""),
            "narrative_score": data.get("narrative_score", ""),
            "narrative_quality": data.get("narrative_quality", ""),
            "narrative_text": data.get("narrative_text", ""),
            "alignment": data.get("alignment", ""),
            "structure": data.get("structure", ""),
            "structure_strength": data.get("structure_strength", ""),
            "trend_persistence": data.get("trend_persistence", ""),
            "choch": data.get("choch", ""),
            "choch_score": data.get("choch_score", ""),
            "consensus": data.get("consensus", ""),
            "consensus_score": data.get("consensus_score", ""),
            "cycle": data.get("cycle", ""),
            "hierarchy_alignment": data.get("hierarchy_alignment", ""),
            "sync_state": data.get("sync_state", ""),
            "context_score": data.get("context_score", ""),
            "environment": data.get("environment", ""),

            "regime": data.get("regime", ""),
            "regime_quality": data.get("regime_quality", ""),
            "regime_score": data.get("regime_score", ""),

            "resultat": data.get("result") or data.get("resultat") or "OPEN"
        }

        colonnes = [
            "trade_id", "date", "symbol", "timeframe", "direction", "action", "mode", "setup",
            "score", "session", "entry", "sl", "tp", "rr", "htf_trend", "bos",
            "fvg", "sweep", "version", "fingerprint", "cis", "cis_score",
            "context_class", "learning_tag", "institutional_ai_label",
            "institutional_ai_score", "institutional_confidence_label",
            "institutional_confidence_score", "dominant_scenario",
            "institutional_rank", "institutional_rank_score", "institutional_pattern",
            "institutional_pattern_quality", "institutional_outcome_key",
            "institutional_outcome_group", "context_scorecard",
            "context_scorecard_value", "tvs", "tvs_score", "penalty",
            "penalty_score", "exit_mode", "management_mode", "be_state", "thesis",
            "setup_id", "setup_family", "learning_bucket", "learning_bucket_score",
            "historical_context_group", "historical_context_quality",
            "edge_stability", "edge_stability_score", "scenario_confidence",
            "bull_scenario", "bull_confidence", "bear_scenario", "bear_confidence",
            "narrative_phase", "narrative_score", "narrative_quality",
            "narrative_text", "alignment", "structure", "structure_strength",
            "trend_persistence", "choch", "choch_score", "consensus",
            "consensus_score", "cycle", "hierarchy_alignment", "sync_state",
            "context_score", "environment", "tvs", "tvs_score", "penalty",
            "penalty_score", "exit_mode", "management_mode", "be_state", "thesis",
            "regime", "regime_quality", "regime_score", "v49_memory_score",
            "v49_memory_quality", "v49_memory_bias", "v49_memory_fingerprint",
            "v49_liquidity_memory_bias", "v49_liquidity_memory_score",
            "v49_fvg_bias", "v49_fvg_memory_score", "v49_htf_level_memory",
            "v49_htf_level_score", "resultat"
        ]

        with open(fichier, mode="a", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=colonnes)

            if file.tell() == 0:
                writer.writeheader()

            writer.writerow(trade)

        google_payload = dict(trade)

        google_payload["webhook_secret"] = GOOGLE_SCRIPT_SECRET

        response = requests.post(
            GOOGLE_SCRIPT_URL,
            json=google_payload,
            timeout=15
        )

        print("Trade reçu :", trade, flush=True)
        print("STATUS GOOGLE SHEETS =", response.status_code, flush=True)
        print("REPONSE GOOGLE SHEETS =", response.text, flush=True)

    except Exception as e:
        print("BACKGROUND ERROR =", str(e), flush=True)


@app.route("/download")
def download_csv():
    return send_file("journal_trades.csv", as_attachment=True)


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8080))
    )
