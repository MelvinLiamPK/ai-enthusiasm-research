#!/bin/bash
#
# SLURM job script for find_urls.py on Sherlock
# Searches Google Custom Search API for LinkedIn profile URLs.
#
# Usage:
#   sbatch jobs/find_urls.sbatch              # Normal run (resume from checkpoint)
#   sbatch jobs/find_urls.sbatch --from-start # Fresh start (ignore checkpoints)
#
# Submit once per day — the script hits the 10k daily API quota in ~4 hours,
# saves a checkpoint, and exits cleanly. Resubmit the next day to continue.
#
#SBATCH --job-name=find_urls
#SBATCH --output=slurm-logs/find_urls_%j.out
#SBATCH --error=slurm-logs/find_urls_%j.err
#SBATCH --time=08:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --partition=normal
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=ml2068@stanford.edu

# ──────────────────────────────────────────────
# Setup
# ──────────────────────────────────────────────

# Navigate to project root
cd "$HOME/ai-enthusiasm-research" || { echo "Project directory not found"; exit 1; }

# Create log directory if needed
mkdir -p slurm-logs

# Load Python and activate virtual environment
ml load python/3.9.0
source venv/bin/activate

# Verify dependencies are available
python3 -c "import pandas, requests, dotenv" 2>/dev/null || {
    echo "ERROR: Missing Python dependencies. Run:"
    echo "  source venv/bin/activate && pip install pandas requests python-dotenv"
    exit 1
}

# ──────────────────────────────────────────────
# Run
# ──────────────────────────────────────────────

INPUT="data/extracted/combined/all_people.csv"

echo "============================================"
echo "Job started: $(date)"
echo "Host: $(hostname)"
echo "============================================"

# Check status before running
python3 src/data_collection/find_urls.py --input "$INPUT" --status

echo ""
echo "Starting search (resume mode)..."
echo ""

# --resume: picks up from last checkpoint
# --yes:    skip confirmation prompt (non-interactive)
python3 src/data_collection/find_urls.py --input "$INPUT" --resume --yes

EXIT_CODE=$?

echo ""
echo "============================================"
echo "Job finished: $(date)"
echo "Exit code: $EXIT_CODE"
echo "============================================"

# Show final status
python3 src/data_collection/find_urls.py --input "$INPUT" --status
