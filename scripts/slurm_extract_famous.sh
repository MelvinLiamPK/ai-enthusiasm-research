#!/bin/bash
#SBATCH --job-name=famous_posts
#SBATCH --partition=nbloom
#SBATCH --mem=32G
#SBATCH --time=00:30:00
#SBATCH --output=logs/famous_posts_%j.out
#SBATCH --error=logs/famous_posts_%j.err

PROJECT_ROOT="$SLURM_SUBMIT_DIR"
module load python/3.12
source "$PROJECT_ROOT/venv/bin/activate"
pip install openpyxl --quiet 2>/dev/null

python3 "$PROJECT_ROOT/extract_famous_posts.py"
