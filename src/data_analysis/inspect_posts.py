"""Inspect post content and data quality for batch 2."""
import pandas as pd
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

csv_path = PROJECT_ROOT / "data/processed/all_people_linkedin_urls/scraped_posts_batch2/posts_20260304_221328.csv"

df = pd.read_csv(csv_path, engine="c", lineterminator="\n",
                 on_bad_lines="skip", low_memory=False)

# ======================================================================
# 1. Sample posts - show actual content
# ======================================================================
print("=" * 70)
print("SAMPLE POSTS (5 random with text)")
print("=" * 70)
has_text = df[df["post_text"].notna() & (df["post_text"].str.len() > 50)]
sample = has_text.sample(5, random_state=42)
for _, row in sample.iterrows():
    print(f"\n{'—' * 50}")
    print(f"Person:   {row['person_name']}  ({row['position']})")
    print(f"Company:  {row['company_name']} ({row['ticker']})")
    print(f"Date:     {row['post_date']}")
    print(f"Type:     {row['post_type']}")
    print(f"Likes:    {row['likes']:.0f}   Comments: {row['comments']:.0f}   Reposts: {row['reposts']:.0f}")
    text = str(row["post_text"])[:300]
    print(f"Text:     {text}{'...' if len(str(row['post_text'])) > 300 else ''}")
    print(f"URL:      {row['post_url']}")

# ======================================================================
# 2. Most active posters
# ======================================================================
print(f"\n\n{'=' * 70}")
print("TOP 20 MOST ACTIVE POSTERS")
print("=" * 70)
counts = df.drop_duplicates(subset=["post_url"]).groupby("profile_url").size().sort_values(ascending=False)
top20 = counts.head(20)

# Get names for top posters
name_map = df.drop_duplicates(subset=["profile_url"]).set_index("profile_url")[["person_name", "company_name"]].to_dict("index")
for url, count in top20.items():
    info = name_map.get(url, {})
    name = info.get("person_name", "?")
    company = info.get("company_name", "?")
    capped = " ← CAPPED" if count == 1000 else ""
    print(f"  {count:>5,} posts  {name:40s}  {company}{capped}")

# ======================================================================
# 3. Post type distribution
# ======================================================================
print(f"\n\n{'=' * 70}")
print("POST TYPE DISTRIBUTION")
print("=" * 70)
type_counts = df["post_type"].value_counts()
for ptype, count in type_counts.items():
    print(f"  {ptype:20s}: {count:>10,} ({count/len(df)*100:.1f}%)")

# ======================================================================
# 4. Post text length distribution
# ======================================================================
print(f"\n\n{'=' * 70}")
print("POST TEXT LENGTH")
print("=" * 70)
text_lens = df["post_text"].dropna().str.len()
print(f"  Mean:   {text_lens.mean():.0f} chars")
print(f"  Median: {text_lens.median():.0f} chars")
print(f"  Max:    {text_lens.max():,.0f} chars")
print(f"  Empty/null: {df['post_text'].isna().sum() + (df['post_text'] == '').sum():,}")

# ======================================================================
# 5. Posts by year
# ======================================================================
print(f"\n\n{'=' * 70}")
print("POSTS BY YEAR")
print("=" * 70)
df["year"] = pd.to_datetime(df["post_date"], errors="coerce").dt.year
year_counts = df["year"].value_counts().sort_index()
for year, count in year_counts.items():
    if pd.notna(year):
        bar = "█" * int(count / year_counts.max() * 40)
        print(f"  {int(year)}: {count:>8,}  {bar}")

# ======================================================================
# 6. Profiles capped at 1000
# ======================================================================
print(f"\n\n{'=' * 70}")
print("PROFILES CAPPED AT 1000 POSTS")
print("=" * 70)
unique_counts = df.drop_duplicates(subset=["post_url"]).groupby("profile_url").size()
capped_1000 = unique_counts[unique_counts == 1000]
print(f"  Count: {len(capped_1000)}")
if len(capped_1000) > 0:
    for url in capped_1000.index[:10]:
        info = name_map.get(url, {})
        name = info.get("person_name", "?")
        print(f"    {name:40s}  {url}")
    if len(capped_1000) > 10:
        print(f"    ... and {len(capped_1000) - 10} more")

# ======================================================================
# 7. Reshared posts check
# ======================================================================
print(f"\n\n{'=' * 70}")
print("RESHARED POSTS")
print("=" * 70)
has_reshare = df["reshared_text"].notna()
print(f"  Posts with reshared content: {has_reshare.sum():,} ({has_reshare.mean()*100:.1f}%)")
print(f"  Posts with reshared URL: {df['reshared_url'].notna().sum():,}")
print(f"  Posts with reshared author: {df['reshared_author'].notna().sum():,}")

# Sample reshared
reshared = df[has_reshare & (df["reshared_text"].str.len() > 50)].sample(min(3, has_reshare.sum()), random_state=99)
for _, row in reshared.iterrows():
    print(f"\n  {'—' * 40}")
    print(f"  Director: {row['person_name']}")
    print(f"  Their text: {str(row['post_text'])[:150]}...")
    print(f"  Reshared: {str(row['reshared_text'])[:150]}...")
    print(f"  Original author: {row['reshared_author']}")

# ======================================================================
# 8. AI keyword quick check
# ======================================================================
print(f"\n\n{'=' * 70}")
print("AI KEYWORD QUICK CHECK")
print("=" * 70)
import re
ai_pattern = re.compile(
    r"\b(artificial intelligence|machine learning|deep learning|generative ai|chatgpt|"
    r"large language model|neural network|ai(?:\s+(?:tool|model|system|agent|powered|driven)))\b",
    re.IGNORECASE
)
text_col = df["post_text"].fillna("")
ai_matches = text_col.apply(lambda x: bool(ai_pattern.search(str(x))))
print(f"  Posts mentioning AI: {ai_matches.sum():,} ({ai_matches.mean()*100:.1f}%)")
print(f"  Profiles with AI posts: {df[ai_matches]['profile_url'].nunique():,}")

# Sample AI posts
ai_posts = df[ai_matches].sample(min(3, ai_matches.sum()), random_state=7)
for _, row in ai_posts.iterrows():
    print(f"\n  {'—' * 40}")
    print(f"  {row['person_name']} ({row['company_name']})")
    print(f"  {str(row['post_text'])[:200]}...")