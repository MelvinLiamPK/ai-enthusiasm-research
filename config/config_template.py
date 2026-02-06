# =========================
# Configuration Template
# =========================
# Copy this file to config.py and fill in your actual credentials
# NEVER commit config.py to version control!

# WRDS Credentials
WRDS_USERNAME = "your_wrds_username"

# =========================
# Google Custom Search API (for LinkedIn URL finding)
# =========================
# Setup:
# 1. Go to https://console.cloud.google.com/
# 2. Create a project and enable "Custom Search API"
# 3. Create an API key at https://console.cloud.google.com/apis/credentials
# 4. Create a Custom Search Engine at https://programmablesearchengine.google.com/
#    - Add "linkedin.com" as the site to search
#    - Get your Search Engine ID (cx)
#
# Free tier: 100 queries/day
# Paid: $5 per 1000 queries after free tier

GOOGLE_API_KEY = "your_google_api_key_here"
GOOGLE_SEARCH_ENGINE_ID = "your_search_engine_id_here"  # Also called "cx"

# =========================
# Apify API (for LinkedIn post scraping)
# =========================
# Get your token at: https://console.apify.com/account/integrations
APIFY_API_TOKEN = "your_apify_token_here"

# =========================
# Other API keys (add as needed)
# =========================
# SERP_API_KEY = "your_serpapi_key"
# OPENAI_API_KEY = "your_openai_key"  # If using for sentiment analysis

# =========================
# Project Settings
# =========================
DEFAULT_BATCH_SIZE = 1000
OUTPUT_DIR = "../data/processed/"
