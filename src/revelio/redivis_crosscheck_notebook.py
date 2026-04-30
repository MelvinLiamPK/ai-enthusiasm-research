"""
Redivis notebook script: cross-check LinkedIn URLs against Revelio data
and export a minimal summary table (no raw Revelio bulk data exported).

Run this cell-by-cell inside a Redivis notebook attached to a workflow
that has these inputs:
  - urls_for_redivis   (your uploaded dataset, table: urls_for_redivis)
  - individual_user    (Revelio Labs Workforce Data v6.0)
  - individual_position (Revelio Labs Workforce Data v6.0)

Output: a small CSV (~58k rows) with only derived boolean columns —
no raw Revelio text fields. Export this from the notebook.

Minimum company name length for matching (avoids "Gap", "3M" etc)
"""

# %% Cell 1 — imports
import re
import redivis
import pandas as pd

MIN_COMPANY_NAME_LEN = 4
MIN_POSTS_FOR_RATIO = 5

# %% Cell 2 — load our URL list from the uploaded dataset
# Replace "melvinliam" with your Redivis username if different
urls_table = redivis.user("melvinliam").dataset("urls_for_redivis").table("urls_for_redivis")
urls_df = urls_table.to_pandas_dataframe()
print(f"Our URLs: {len(urls_df):,} rows")
print(urls_df.head(3))

# %% Cell 3 — join URLs → individual_user (within this workflow's scope)
# This runs server-side; only matched rows are returned
matched_users_df = redivis.query("""
    SELECT
        u.user_id,
        u.firstname,
        u.lastname,
        u.fullname,
        u.profile_linkedin_url,
        u.profile_title,
        u.numconnections,
        u.user_country,
        u.prestige,
        urls.clean_linkedin_url
    FROM urls_for_redivis AS urls
    INNER JOIN individual_user AS u
        ON urls.clean_linkedin_url = u.profile_linkedin_url
""").to_pandas_dataframe()

print(f"Matched users: {len(matched_users_df):,} rows")
print(matched_users_df.head(3))

# %% Cell 4 — fetch positions for matched user_ids only
# Pull only company_cleaned + seniority to keep this small
user_ids = matched_users_df["user_id"].dropna().astype(int).tolist()
print(f"Fetching positions for {len(user_ids):,} user_ids...")

# Redivis query with IN clause — works for up to ~50k values
ids_str = ", ".join(str(i) for i in user_ids)

positions_df = redivis.query(f"""
    SELECT
        user_id,
        company_cleaned,
        seniority,
        startdate,
        enddate
    FROM individual_position
    WHERE user_id IN ({ids_str})
""").to_pandas_dataframe()

print(f"Positions fetched: {len(positions_df):,} rows")
print(positions_df.head(3))

# %% Cell 5 — build positions index {user_id: [company_cleaned, ...]}
positions_index = {}
for row in positions_df[["user_id", "company_cleaned"]].itertuples(index=False):
    uid = row.user_id
    if uid not in positions_index:
        positions_index[uid] = []
    positions_index[uid].append(row.company_cleaned)

print(f"Position index built for {len(positions_index):,} users")

# %% Cell 6 — helper functions (identical logic to revelio_crosscheck.py)

def name_matches(revelio_fullname, our_name):
    if pd.isna(revelio_fullname) or pd.isna(our_name):
        return False
    last = str(our_name).lower().split()[-1] if str(our_name).strip() else ""
    return last in str(revelio_fullname).lower()


def company_in_positions(user_id, company_name):
    if pd.isna(company_name) or len(str(company_name)) < MIN_COMPANY_NAME_LEN:
        return False
    positions = positions_index.get(int(user_id), [])
    co = str(company_name).lower()
    for pos in positions:
        if pd.isna(pos):
            continue
        if co in str(pos).lower() or str(pos).lower() in co:
            return True
    return False


# %% Cell 7 — build lookup: clean_url → revelio user row
revelio_by_url = matched_users_df.drop_duplicates("clean_linkedin_url").set_index("clean_linkedin_url")

# %% Cell 8 — load our full all_linkedin_urls data for name/company info
# We need person_name and company_name_clean — upload all_linkedin_urls.csv
# as a second dataset, OR just use the name columns from urls_for_redivis
# if you added them. Simplest: upload all_people.csv as a dataset.
#
# For now, load it from your uploaded urls dataset if it has those columns,
# otherwise replace with a second uploaded table.

# If all_people.csv is uploaded as "all_people" table:
all_people_table = redivis.user("melvinliam").dataset("all_people").table("all_people")
all_people_df = all_people_table.to_pandas_dataframe(
    variables=["person_name", "company_name", "company_name_clean", "source", "linkedin_url"]
)
print(f"all_people: {len(all_people_df):,} rows")

# %% Cell 9 — normalise URLs in all_people_df for join
def clean_url(url):
    if pd.isna(url):
        return None
    url = str(url).strip().rstrip("/")
    url = re.sub(r"^https?://(www\.)?", "", url)
    return url if url.startswith("linkedin.com/in/") else None

all_people_df["_clean_url"] = all_people_df["linkedin_url"].apply(clean_url)

# %% Cell 10 — compute confirmation columns
revelio_url_match = []
revelio_name_confirmed = []
revelio_company_confirmed = []
revelio_user_id_col = []

for row in all_people_df.itertuples(index=False):
    clean = row._clean_url
    rev = revelio_by_url.get(clean) if clean and clean in revelio_by_url.index else None

    if rev is None:
        revelio_url_match.append(False)
        revelio_name_confirmed.append(False)
        revelio_company_confirmed.append(False)
        revelio_user_id_col.append(None)
    else:
        revelio_url_match.append(True)
        uid = rev["user_id"]
        revelio_user_id_col.append(uid)
        revelio_name_confirmed.append(name_matches(rev["fullname"], row.person_name))
        revelio_company_confirmed.append(company_in_positions(uid, row.company_name_clean))

all_people_df["revelio_url_match"] = revelio_url_match
all_people_df["revelio_name_confirmed"] = revelio_name_confirmed
all_people_df["revelio_company_confirmed"] = revelio_company_confirmed
all_people_df["revelio_user_id"] = revelio_user_id_col

# %% Cell 11 — summary stats
total = len(all_people_df)
found = all_people_df["_clean_url"].notna().sum()
matched = sum(revelio_url_match)
name_conf = sum(revelio_name_confirmed)
co_conf = sum(revelio_company_confirmed)
both = sum(a and b for a, b in zip(revelio_name_confirmed, revelio_company_confirmed))

print(f"Total rows:               {total:>8,}")
print(f"Has URL:                  {found:>8,}")
print(f"Revelio URL match:        {matched:>8,}  ({matched/found*100:.1f}% of found)")
print(f"  Name confirmed:         {name_conf:>8,}  ({name_conf/matched*100:.1f}% of matched)")
print(f"  Company confirmed:      {co_conf:>8,}  ({co_conf/matched*100:.1f}% of matched)")
print(f"  Both confirmed:         {both:>8,}  ({both/matched*100:.1f}% of matched)")

# %% Cell 12 — export minimal summary (no raw Revelio text fields)
output = all_people_df[[
    "linkedin_url",
    "person_name",
    "company_name_clean",
    "source",
    "revelio_url_match",
    "revelio_name_confirmed",
    "revelio_company_confirmed",
    "revelio_user_id",
]].copy()

output.to_csv("revelio_validation_summary.csv", index=False)
print(f"Exported revelio_validation_summary.csv ({len(output):,} rows)")
print("Download this file from the notebook environment.")
