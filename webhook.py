from flask import Flask, request, send_file
import csv
import requests
from datetime import datetime

app = Flask(__name__)

fichier = "journal_trades.csv"
GOOGLE_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbyVUSrnvHzkFTyuMqZfjEvXbOe2_bkFRsCYbdtfXuR3MZgGJePgh5vF9-eqeHnCeDCq/exec"

@app.route("/webhook", methods=["POST"])
def webhook():

    data = request.get_json(force=True)

    trade = {
    "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "symbol": data.get("symbol", ""),
    "timeframe": data.get("timeframe", ""),
    "direction": data.get("direction", ""),
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
        
    "resultat": data.get("result") or data.get("resultat") or "OPEN"
}

    with open(fichier, mode="a", newline="") as file:

        colonnes = [
    "date",
    "symbol",
    "timeframe",
    "direction",
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
    
    "resultat"
]

        writer = csv.DictWriter(file, fieldnames=colonnes)

        if file.tell() == 0:
            writer.writeheader()

        writer.writerow(trade)
        
        try:
            response = requests.post(
                GOOGLE_SCRIPT_URL,
                json=trade,
                timeout=2
            )
            
            print("STATUS GOOGLE SHEETS =",response.status_code)
            print("REPONSE GOOGLE SHEETS =",response.text)
        
        except Exception as e:
            print("ERREUR GOOGLE SHEETS =", e)

        print("Trade reçu :", trade)

        return {
            "message": "Trade enregistré"
        }

@app.route("/download")
def download_csv():
    return send_file("journal_trades.csv", as_attachment=True)

app.run(host="0.0.0.0", port=80)
