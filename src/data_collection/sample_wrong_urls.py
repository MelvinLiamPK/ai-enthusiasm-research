import pandas as pd

file_path = '/Users/melvinliam/Documents/Uni/RA-NB/Scraping/ai-enthusiasm-research/data/processed/sp500_linkedin_urls/all_sp500_linkedin_urls.csv'
df = pd.read_csv(file_path)

# Check what's in the verified column
print("Verified column dtype:", df['verified'].dtype)
print("Unique values:", df['verified'].unique()[:10])

# Filter using string comparison instead
unverified = df[(df['verified'].astype(str) == 'False') & (df['linkedin_url'].notna())].copy()

print(f"\nUnverified rows: {len(unverified)}")

# Sample
if len(unverified) > 0:
    sample = unverified.sample(min(20, len(unverified)), random_state=42)
    for _, row in sample.iterrows():
        print(f"\nDirector: {row['director_name_clean']}")
        print(f"Company:  {row['company_name_clean']}")
        print(f"LinkedIn: {row['linkedin_title']}")