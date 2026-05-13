"""
Build Serper input CSV from DEF 14A extracted bios for re-search testing.

Extracts specific directors from parsed bios and prepares them for
LinkedIn URL re-discovery using primary company as the anchor.
"""

import pandas as pd
import random
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# These are the 10 directors from the pseudorandom sample (seed=7)
TARGET_DIRECTORS = [
    ("John O. Dabiri", "NVDA", "California Institute of Technology"),
    ("Roger W. Ferguson Jr.", "GOOGL", "Red Cell Partners LLC"),
    ("Stephen C. Neal", "NVDA", "Cooley LLP"),
    ("Alicia Boler Davis", "JPM", "Alto Pharmacy, LLC"),
    ("Sue Wagner", "AAPL", "BlackRock, Inc."),
    ("Sundar Pichai", "GOOGL", "Alphabet"),
    ("Carla A. Harris", "WMT", "Morgan Stanley"),
    ("James Murdoch", "TSLA", "Lupa Systems"),
    ("Frances H. Arnold", "GOOGL", "California Institute of Technology"),
    ("Harvey C. Jones", "NVDA", "Square Wave Ventures"),
]

# Load parsed bios
bios_path = PROJECT_ROOT / "data" / "processed" / "def14a_extracted_bios.csv"
df_bios = pd.read_csv(bios_path)

# Extract the 10 target directors
results = []
for name, ticker, primary_company in TARGET_DIRECTORS:
    # Find in bios (match on name, ticker, primary_company)
    row = df_bios[
        (df_bios["full_name"].str.strip() == name.strip()) &
        (df_bios["ticker"] == ticker) &
        (df_bios["primary_company"].notna())
    ]

    if len(row) > 0:
        row = row.iloc[0]
        results.append({
            "person_name": row["full_name"],
            "company": row["primary_company"],  # Use primary company for Serper search
            "ticker": ticker,
            "primary_company": row["primary_company"],
            "primary_role": row["primary_role"],
            "is_current": row["is_current"],
            "source": "def14a_extracted",
        })
    else:
        print(f"Warning: Could not find {name} | {ticker} | {primary_company}")

df_input = pd.DataFrame(results)

# Save to CSV
output_path = PROJECT_ROOT / "data" / "processed" / "def14a_serper_input_prototype.csv"
output_path.parent.mkdir(parents=True, exist_ok=True)
df_input.to_csv(output_path, index=False)

print(f"\nCreated input CSV: {output_path}")
print(f"  {len(df_input)} directors ready for Serper re-search")
print(f"\nColumns: {', '.join(df_input.columns)}")
print(f"\nSample:")
print(df_input.head(3))
