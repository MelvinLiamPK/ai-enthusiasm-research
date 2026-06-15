#!/usr/bin/env python3
"""Report actual Apify run usage (USD) + item counts for the posts-scraper actor.
Run on Sherlock (token in .env). Used to get an accurate $/post rate for cost projection."""
import os
from apify_client import ApifyClient

for line in open(".env"):
    if line.startswith("APIFY_API_TOKEN"):
        os.environ["APIFY_API_TOKEN"] = line.split("=", 1)[1].strip().strip('"').strip("'")

c = ApifyClient(os.environ["APIFY_API_TOKEN"])
ACTOR = "apimaestro/linkedin-batch-profile-posts-scraper"
runs = c.actor(ACTOR).runs().list(limit=60, desc=True).items

print(f"{'started':20s} {'status':11s} {'usd':>8s} {'posts':>8s}  runId")
tot = 0.0
titems = 0
for r in runs:
    st = str(r.get("startedAt"))[:19]
    if st[:10] not in ("2026-06-13", "2026-06-14"):
        continue
    usd = r.get("usageTotalUsd") or 0.0
    try:
        items = c.dataset(r["defaultDatasetId"]).get()["itemCount"]
    except Exception:
        items = -1
    status = r.get("status") or "?"
    rid = r.get("id")
    print(f"{st:20s} {status:11s} {usd:8.3f} {items:8d}  {rid}")
    tot += usd
    if items > 0:
        titems += items

print(f"\nTODAY total USD: {tot:.2f} | total posts(items): {titems:,}")
if titems:
    print(f"implied $/post: {tot/titems:.5f}")

# monthly usage (account-level) if available
try:
    me = c.user("me").get()
    print("\naccount monthly usage:", me.get("plan", {}).get("monthlyUsageUsd"), "/ limit",
          me.get("plan", {}).get("maxMonthlyUsageUsd"))
except Exception as e:
    print("account usage lookup failed:", e)
