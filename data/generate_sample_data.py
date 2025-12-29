import csv
from datetime import datetime, timedelta
import random

# Configuration
symbols = ["SP500", "STOXX600", "NIKKEI225"]
start_date = datetime(2025, 12, 1, 9, 0, 0)
num_rows = 150  # 50 par symbole

rows = []
for symbol in symbols:
    current_time = start_date
    base_price = random.uniform(3000, 5000)

    for i in range(50):
        price = base_price + random.uniform(-50, 50)
        volume = random.randint(100000, 500000)

        rows.append(
            {
                "timestamp": current_time.isoformat(),
                "symbol": symbol,
                "price": round(price, 2),
                "volume": volume,
            }
        )

        current_time += timedelta(minutes=5)

# Écrire le CSV
with open("data/indices_sample.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["timestamp", "symbol", "price", "volume"])
    writer.writeheader()
    writer.writerows(rows)

print(f"✅ Fichier généré : data/indices_sample.csv ({len(rows)} lignes)")
