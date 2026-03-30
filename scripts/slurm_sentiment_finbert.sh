#!/bin/bash
#SBATCH --job-name=sentiment_fb
#SBATCH --partition=gpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --time=06:00:00
#SBATCH --gres=gpu:1
#SBATCH --output=logs/sentiment_fb_%j.out
#SBATCH --error=logs/sentiment_fb_%j.err

# ============================================================
# FinBERT Sentiment Analysis — SLURM wrapper (GPU)
# Submit from project root: sbatch scripts/slurm_sentiment_finbert.sh
# ============================================================

set -e

PROJECT_ROOT="$SLURM_SUBMIT_DIR"

echo "Project root: $PROJECT_ROOT"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $(hostname)"
echo "Start: $(date)"
echo "Memory requested: $SLURM_MEM_PER_NODE MB"
echo "GPU: $(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null || echo 'none detected')"
echo "============================================================"

mkdir -p "$PROJECT_ROOT/logs"

# Use scratch for HuggingFace model cache (avoids filling home quota)
export HF_HOME="$SCRATCH/huggingface_cache"
echo "HF_HOME: $HF_HOME"

# Load modules (py312 versions match python/3.12; includes CUDA support)
module load python/3.12
module load py-pytorch/2.4.1_py312
module load py-transformers/4.39.1_py312

# Activate virtual environment (for other project deps)
if [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
    source "$PROJECT_ROOT/venv/bin/activate"
    echo "Activated venv"
elif [ -f "$HOME/.venvs/ai-enthusiasm/bin/activate" ]; then
    source "$HOME/.venvs/ai-enthusiasm/bin/activate"
    echo "Activated ~/.venvs/ai-enthusiasm"
fi

# Full scoring — reads most recent L-M output by default, appends FinBERT columns
python3 "$PROJECT_ROOT/src/data_analysis/sentiment_finbert.py" \
    --output-dir "$PROJECT_ROOT/outputs/sentiment_results" \
    --chunk-size 50000 \
    --batch-size 64

echo "============================================================"
echo "End: $(date)"
echo "Exit code: $?"

# ============================================================
# ALTERNATIVE: AI-only mode (much faster, scores ~130K posts)
# ============================================================
# python3 "$PROJECT_ROOT/src/data_analysis/sentiment_finbert.py" \
#     --output-dir "$PROJECT_ROOT/outputs/sentiment_results" \
#     --ai-only --batch-size 64

# ============================================================
# RESUME: If the job was interrupted, resubmit with --resume
# ============================================================
# python3 "$PROJECT_ROOT/src/data_analysis/sentiment_finbert.py" \
#     --output-dir "$PROJECT_ROOT/outputs/sentiment_results" \
#     --chunk-size 50000 --batch-size 64 --resume
