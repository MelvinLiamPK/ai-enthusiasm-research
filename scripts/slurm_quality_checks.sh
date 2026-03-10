#!/bin/bash
#SBATCH --job-name=quality_checks
#SBATCH --partition=nbloom
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --time=02:00:00
#SBATCH --output=logs/quality_checks_%j.out
#SBATCH --error=logs/quality_checks_%j.err

# ============================================================
# Data Quality Checks — SLURM wrapper
# Submit from project root: sbatch scripts/slurm_quality_checks.sh
# ============================================================

set -e

# SLURM_SUBMIT_DIR = directory where sbatch was called (should be project root)
PROJECT_ROOT="$SLURM_SUBMIT_DIR"

echo "Project root: $PROJECT_ROOT"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $(hostname)"
echo "Start: $(date)"
echo "Memory requested: $SLURM_MEM_PER_NODE MB"
echo "============================================================"

# Create logs directory if needed
mkdir -p "$PROJECT_ROOT/logs"

# Activate virtual environment if it exists
if [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
    source "$PROJECT_ROOT/venv/bin/activate"
    echo "Activated venv"
elif [ -f "$HOME/.venvs/ai-enthusiasm/bin/activate" ]; then
    source "$HOME/.venvs/ai-enthusiasm/bin/activate"
    echo "Activated ~/.venvs/ai-enthusiasm"
fi

# Run quality checks with all reference files
python3 "$PROJECT_ROOT/src/data_analysis/data_quality_checks.py" \
    --posts "$PROJECT_ROOT/data/processed/all_people_linkedin_urls/scraped_posts_combined/posts_combined.csv" \
    --urls "$PROJECT_ROOT/data/processed/all_people_linkedin_urls/all_linkedin_urls.csv" \
    --directors "$PROJECT_ROOT/data/raw/directors.csv" \
    --output-dir "$PROJECT_ROOT/outputs/quality_checks"

echo "============================================================"
echo "End: $(date)"
echo "Exit code: $?"