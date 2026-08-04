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

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True)

    Thread(
        target=process_webhook_background,
        args=(data,),
        daemon=True
    ).start()

    return jsonify({
        "status": "received",
        "message": "Webhook received by Railway"
    }), 200


def process_webhook_background(data):
    try:
        if not GOOGLE_SCRIPT_SECRET:
            raise RuntimeError(
                "GOOGLE_SCRIPT_SECRET manquant dans Railway Variables"
            )
        
        trade = {
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
            "date", "symbol", "timeframe", "direction", "action", "mode", "setup",
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


app.run(host="0.0.0.0", port=80)
