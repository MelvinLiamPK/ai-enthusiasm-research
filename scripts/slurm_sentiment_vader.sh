#!/bin/bash
#SBATCH --job-name=sentiment_vd
#SBATCH --partition=nbloom
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --time=06:00:00
#SBATCH --output=logs/sentiment_vd_%j.out
#SBATCH --error=logs/sentiment_vd_%j.err

# ============================================================
# VADER Sentiment Analysis — SLURM wrapper (CPU only)
# Submit from project root: sbatch scripts/slurm_sentiment_vader.sh
# ============================================================

set -e

PROJECT_ROOT="$SLURM_SUBMIT_DIR"

echo "Project root: $PROJECT_ROOT"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $(hostname)"
echo "Start: $(date)"
echo "Memory requested: $SLURM_MEM_PER_NODE MB"
echo "============================================================"

mkdir -p "$PROJECT_ROOT/logs"

# Load Python
module load python/3.12

# Activate virtual environment
if [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
    source "$PROJECT_ROOT/venv/bin/activate"
    echo "Activated venv"
elif [ -f "$HOME/.venvs/ai-enthusiasm/bin/activate" ]; then
    source "$HOME/.venvs/ai-enthusiasm/bin/activate"
    echo "Activated ~/.venvs/ai-enthusiasm"
fi

# Install VADER (tiny, ~1MB)
pip install --no-cache-dir vaderSentiment

# Full scoring — reads most recent scored file, appends VADER columns
python3 "$PROJECT_ROOT/src/data_analysis/sentiment_vader.py" \
    --output-dir "$PROJECT_ROOT/outputs/sentiment_results" \
    --chunk-size 100000

echo "============================================================"
echo "End: $(date)"
echo "Exit code: $?"
