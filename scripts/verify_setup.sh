#!/bin/bash
# ==========================================================
# Verify Sherlock setup for LinkedIn scraping
# ==========================================================
#
# Run this BEFORE submitting any SLURM jobs:
#   bash scripts/verify_setup.sh
#
# ==========================================================

set -euo pipefail

PROJECT_DIR="$HOME/ai-enthusiasm-research"
PASS=0
FAIL=0

check() {
    local label="$1"
    local result="$2"
    if [ "$result" = "ok" ]; then
        echo "  ✓ $label"
        PASS=$((PASS + 1))
    else
        echo "  ✗ $label — $result"
        FAIL=$((FAIL + 1))
    fi
}

echo "============================================================"
echo "Sherlock Setup Verification"
echo "============================================================"
echo ""

# --- 1. Project directory ---
echo "1. Project directory"
if [ -d "$PROJECT_DIR" ]; then
    check "Project dir exists" "ok"
else
    check "Project dir exists" "NOT FOUND: $PROJECT_DIR"
fi

# --- 2. Virtual environment ---
echo ""
echo "2. Virtual environment"
if [ -f "$PROJECT_DIR/venv/bin/activate" ]; then
    check "venv exists" "ok"
    source "$PROJECT_DIR/venv/bin/activate"
    check "venv activated" "ok"
    echo "     Python: $(which python3)"
    echo "     Version: $(python3 --version)"
else
    check "venv exists" "NOT FOUND — run: python3 -m venv venv && source venv/bin/activate && pip install apify-client pandas python-dotenv"
fi

# --- 3. Python dependencies ---
echo ""
echo "3. Python dependencies"
for pkg in apify_client pandas dotenv; do
    if python3 -c "import $pkg" 2>/dev/null; then
        check "$pkg installed" "ok"
    else
        check "$pkg installed" "MISSING — pip install $(echo $pkg | sed 's/dotenv/python-dotenv/')"
    fi
done

# --- 4. .env file and API token ---
echo ""
echo "4. API credentials"
ENV_FILE="$PROJECT_DIR/.env"
if [ -f "$ENV_FILE" ]; then
    check ".env file exists" "ok"
    if grep -q "APIFY_API_TOKEN" "$ENV_FILE"; then
        # Check it's not empty/placeholder
        TOKEN=$(grep "APIFY_API_TOKEN" "$ENV_FILE" | head -1 | cut -d'=' -f2 | tr -d ' "'"'"'')
        if [ -n "$TOKEN" ] && [ "$TOKEN" != "your_token_here" ]; then
            check "APIFY_API_TOKEN set" "ok"
            echo "     Token: ${TOKEN:0:8}...${TOKEN: -4} (${#TOKEN} chars)"
        else
            check "APIFY_API_TOKEN set" "EMPTY or placeholder — edit $ENV_FILE"
        fi
    else
        check "APIFY_API_TOKEN set" "NOT FOUND in .env — add: APIFY_API_TOKEN=your_token"
    fi
else
    check ".env file exists" "NOT FOUND at $ENV_FILE"
fi

# --- 5. Apify token validation (quick API call) ---
echo ""
echo "5. Apify API connectivity"
if [ -n "${TOKEN:-}" ] && [ "$TOKEN" != "your_token_here" ]; then
    # Try a lightweight API call to validate the token
    RESULT=$(python3 -c "
from apify_client import ApifyClient
try:
    client = ApifyClient('$TOKEN')
    user = client.user().get()
    print(f\"ok|{user.get('username', 'unknown')}\")
except Exception as e:
    print(f'fail|{e}')
" 2>/dev/null || echo "fail|python error")
    
    STATUS=$(echo "$RESULT" | cut -d'|' -f1)
    DETAIL=$(echo "$RESULT" | cut -d'|' -f2-)
    
    if [ "$STATUS" = "ok" ]; then
        check "Apify API connection" "ok"
        echo "     Apify user: $DETAIL"
    else
        check "Apify API connection" "FAILED — $DETAIL"
    fi
else
    check "Apify API connection" "SKIPPED (no valid token)"
fi

# --- 6. Input data ---
echo ""
echo "6. Input data"
INPUT_FILE="$PROJECT_DIR/data/processed/all_people_linkedin_urls/all_linkedin_urls.csv"
if [ -f "$INPUT_FILE" ]; then
    LINES=$(wc -l < "$INPUT_FILE")
    check "Input CSV exists" "ok"
    echo "     File: $INPUT_FILE"
    echo "     Rows: $LINES (including header)"
else
    check "Input CSV exists" "NOT FOUND at $INPUT_FILE"
fi

# --- 7. Scripts ---
echo ""
echo "7. Scripts"
for script in src/data_collection/scrape_posts.py; do
    if [ -f "$PROJECT_DIR/$script" ]; then
        check "$script exists" "ok"
    else
        check "$script exists" "NOT FOUND"
    fi
done

# --- 8. Output directory ---
echo ""
echo "8. Output directories"
OUTPUT_DIR="$PROJECT_DIR/data/processed/all_people_linkedin_urls/scraped_posts"
mkdir -p "$OUTPUT_DIR" 2>/dev/null
check "Output dir created/exists" "ok"
echo "     $OUTPUT_DIR"

# Logs directory
mkdir -p "$PROJECT_DIR/logs" 2>/dev/null
check "Logs dir created/exists" "ok"

# --- 9. SLURM jobs ---
echo ""
echo "9. SLURM job scripts"
for job in jobs/scrape_posts.sh; do
    if [ -f "$PROJECT_DIR/$job" ]; then
        check "$job exists" "ok"
    else
        check "$job exists" "NOT FOUND"
    fi
done

# --- 10. Quick dry run ---
echo ""
echo "10. Dry run (--stats)"
if [ -f "$PROJECT_DIR/src/data_collection/scrape_posts.py" ] && [ -f "$INPUT_FILE" ]; then
    cd "$PROJECT_DIR"
    python3 src/data_collection/scrape_posts.py --input "$INPUT_FILE" --stats 2>&1 | head -20
    check "Dry run successful" "ok"
else
    check "Dry run" "SKIPPED (missing files)"
fi

# --- Summary ---
echo ""
echo "============================================================"
echo "SUMMARY: $PASS passed, $FAIL failed"
echo "============================================================"

if [ $FAIL -eq 0 ]; then
    echo ""
    echo "All checks passed! Ready to submit:"
    echo "  cd $PROJECT_DIR"
    echo "  sbatch jobs/scrape_posts.sh"
else
    echo ""
    echo "Fix the failures above before submitting jobs."
fi
