#!/bin/bash
#SBATCH --job-name=serper_def14a
#SBATCH --partition=nbloom
#SBATCH --time=06:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16GB
#SBATCH --output=logs/serper_def14a_%j.log
#SBATCH --error=logs/serper_def14a_%j.err

# Load Python and activate venv
module load python/3.12
source venv/bin/activate

# Ensure logs directory exists
mkdir -p logs

# Run Serper re-search on DEF 14A directors using primary_company anchor
# Expected: ~23,060 searches @ ~0.35 sec/search + rate limiting
# Time estimate: ~4–6 hours
# Budget: ~1,000 of 1,019 Serper credits

python3 src/data_collection/find_urls_serper.py \
  --input data/processed/def14a_serper_input_all_directors.csv \
  --run --yes

echo "Serper re-search complete. Check logs for results."
