#!/bin/bash
#SBATCH --job-name=sentiment_lm
#SBATCH --partition=nbloom
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --time=06:00:00
#SBATCH --output=logs/sentiment_lm_%j.out
#SBATCH --error=logs/sentiment_lm_%j.err

# ============================================================
# L-M Sentiment Analysis — SLURM wrapper
# Submit from project root: sbatch scripts/slurm_sentiment_analysis.sh
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

mkdir -p "$PROJECT_ROOT/logs"

# Load Python module (required on compute nodes)
module load python/3.12

# Activate virtual environment
if [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
    source "$PROJECT_ROOT/venv/bin/activate"
    echo "Activated venv"
elif [ -f "$HOME/.venvs/ai-enthusiasm/bin/activate" ]; then
    source "$HOME/.venvs/ai-enthusiasm/bin/activate"
    echo "Activated ~/.venvs/ai-enthusiasm"
fi

# Full scoring (all posts, then slice subsets)
# Chunk size 100K balances memory vs overhead.
# Increase --mem to 96G if OOM on the tagging/subset step.
python3 "$PROJECT_ROOT/src/data_analysis/sentiment_analysis_full.py" \
    --posts "$PROJECT_ROOT/data/processed/all_people_linkedin_urls/scraped_posts_combined/posts_combined.csv" \
    --lm-dict "$PROJECT_ROOT/data/Loughran-McDonald_MasterDictionary_1993-2024.csv" \
    --output-dir "$PROJECT_ROOT/outputs/sentiment_results" \
    --chunk-size 100000

echo "============================================================"
echo "End: $(date)"
echo "Exit code: $?"

# ============================================================
# ALTERNATIVE: AI-only mode (faster, ~30min, 32GB sufficient)
# Uncomment below and comment out the full scoring block above
# ============================================================
# python3 "$PROJECT_ROOT/src/data_analysis/sentiment_analysis_full.py" \
#     --posts "$PROJECT_ROOT/data/processed/all_people_linkedin_urls/scraped_posts_combined/posts_combined.csv" \
#     --lm-dict "$PROJECT_ROOT/data/Loughran-McDonald_MasterDictionary_1993-2024.csv" \
#     --output-dir "$PROJECT_ROOT/outputs/sentiment_results" \
#     --ai-only