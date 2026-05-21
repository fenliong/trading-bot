from flask import Flask, request
import csv
from datetime import datetime

app = Flask(__name__)

fichier = "journal_trades.csv"

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
    "resultat": data.get("resultat", "")
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
    "resultat"
]

        writer = csv.DictWriter(file, fieldnames=colonnes)

        if file.tell() == 0:
            writer.writeheader()

        writer.writerow(trade)

    print("Trade reçu :", trade)

    return {
        "message": "Trade enregistré"
    }

app.run(host="0.0.0.0", port=80)