#!/bin/bash
#SBATCH --job-name=linkedin-scrape-posts
#SBATCH --partition=normal
#SBATCH --time=48:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=1
#SBATCH --output=logs/scrape_posts_%j.log

# ==========================================================
# LinkedIn Post Scraper — SLURM job for Sherlock
# ==========================================================
#
# Usage:
#   sbatch jobs/scrape_posts.sh
#
# Monitor:
#   squeue -u $USER
#   tail -f logs/scrape_posts_<JOBID>.log
#
# ==========================================================

set -euo pipefail

# Project root (resolve relative to this script)
PROJECT_DIR="$HOME/ai-enthusiasm-research"
cd "$PROJECT_DIR"

echo "============================================================"
echo "LinkedIn Post Scraper — SLURM Job"
echo "============================================================"
echo "Job ID:    $SLURM_JOB_ID"
echo "Node:      $(hostname)"
echo "Started:   $(date)"
echo "Project:   $PROJECT_DIR"
echo "============================================================"

# Load Python
module load python/3.12 2>/dev/null || module load python/3.10 2>/dev/null || true

# Activate virtual environment
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
    echo "Venv:      $(which python3)"
else
    echo "ERROR: venv not found at $PROJECT_DIR/venv"
    exit 1
fi

# Ensure log directory exists
mkdir -p logs

# Run scraper
# --yes    = skip confirmation prompts
# --run    = auto-resumes from checkpoint if one exists
python3 src/data_collection/scrape_posts.py \
    --input data/processed/all_people_linkedin_urls/all_linkedin_urls.csv \
    --output data/processed/all_people_linkedin_urls/scraped_posts/ \
    --max-posts 1000 \
    --batch-size 100 \
    --run --yes

echo ""
echo "============================================================"
echo "Finished: $(date)"
echo "============================================================"
