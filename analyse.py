import pandas as pd

# Charger le CSV
df = pd.read_csv("journal_trades.csv")

print("\n===== ANALYSE DES TRADES =====\n")

# Nombre total de trades
total = len(df)
print(f"Total trades : {total}")

# Winrate global
wins = len(df[df["resultat"] == "WIN"])
winrate = (wins / total) * 100 if total > 0 else 0

print(f"Winrate global : {winrate:.2f}%")

# Stats par setup
print("\n===== WINRATE PAR SETUP =====\n")

setups = df["setup"].unique()

for setup in setups:
    subset = df[df["setup"] == setup]

    total_setup = len(subset)
    wins_setup = len(subset[subset["resultat"] == "WIN"])

    wr = (wins_setup / total_setup) * 100 if total_setup > 0 else 0

    print(f"{setup} : {wr:.2f}% ({wins_setup}/{total_setup})")

# Stats par session
print("\n===== WINRATE PAR SESSION =====\n")

sessions = df["session"].unique()

for session in sessions:
    subset = df[df["session"] == session]

    total_session = len(subset)
    wins_session = len(subset[subset["resultat"] == "WIN"])

    wr = (wins_session / total_session) * 100 if total_session > 0 else 0

    print(f"{session} : {wr:.2f}% ({wins_session}/{total_session})")

# Scores élevés
print("\n===== ANALYSE DES SCORES =====\n")

high_score = df[df["score"] >= 90]

if len(high_score) > 0:
    wins_high = len(high_score[high_score["resultat"] == "WIN"])

    wr_high = (wins_high / len(high_score)) * 100

    print(f"Score >= 90 : {wr_high:.2f}%")
else:
    print("Pas assez de données.")