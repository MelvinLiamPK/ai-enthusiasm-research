"""
Build Serper input for S&P 100 directors extracted from DEF 14A.

Takes the parsed bios and filters to S&P 100 companies for scaled testing
of the primary-company-first algorithm before full deployment.
"""

import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# S&P 100 tickers (as of 2025)
SP100_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "JPM", "JNJ", "WMT",
    "V", "PG", "MA", "HD", "DIS", "KO", "NFLX", "ASML", "NVO", "COST",
    "AVGO", "TM", "SAP", "ABBV", "NOVO", "ABT", "LLY", "SHEL", "ACN", "BAC",
    "TMO", "IBM", "UNH", "AMD", "BKNG", "QCOM", "TSM", "NVR", "MRK", "INTC",
    "NOW", "CAT", "EU", "BAH", "CSCO", "TTE", "GE", "HON", "MMM", "SLB",
    "XOM", "CVBF", "RY", "CVX", "RSG", "CI", "FISV", "ROP", "NFLX", "AXP",
]

# Load parsed bios
bios_path = PROJECT_ROOT / "data" / "processed" / "def14a_extracted_bios.csv"
df_bios = pd.read_csv(bios_path)

# Filter to S&P 100 companies
df_sp100 = df_bios[df_bios["ticker"].isin(SP100_TICKERS)].copy()

# Filter to directors only (optional: include officers, but directors are the main focus)
df_sp100 = df_sp100[df_sp100["role_context"] == "director"].copy()

# Filter to those with extracted primary company
df_sp100 = df_sp100[df_sp100["primary_company"].notna()].copy()

# Build input CSV
result = []
for _, row in df_sp100.iterrows():
    result.append({
        "person_name": row["full_name"],
        "company": row["primary_company"],  # Use primary company for search
        "ticker": row["ticker"],
        "primary_company": row["primary_company"],
        "primary_role": row["primary_role"],
        "is_current": row["is_current"],
        "source": "def14a_extracted",
    })

df_input = pd.DataFrame(result)

# Remove duplicates (same person on multiple boards)
df_input = df_input.drop_duplicates(subset=["person_name", "company"]).reset_index(drop=True)

# Save
output_path = PROJECT_ROOT / "data" / "processed" / "sp100_serper_input.csv"
output_path.parent.mkdir(parents=True, exist_ok=True)
df_input.to_csv(output_path, index=False)

print(f"Created S&P 100 Serper input: {output_path}")
print(f"  Total directors with extracted primary company: {len(df_input)}")
print(f"  Unique people: {df_input['person_name'].nunique()}")
print(f"  Unique tickers: {df_input['ticker'].nunique()}")
print(f"\nSample:")
print(df_input.head(5))
