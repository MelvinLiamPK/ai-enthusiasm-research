# Loughran-McDonald Dictionary Setup

## Overview

`analysisAI_LM.py` and `covid_sentiment_LM.py` use the **Loughran-McDonald Master Dictionary** for financial sentiment analysis. This dictionary must be downloaded separately before running the scripts.

## Required Setup

### 1. Download the LM Master Dictionary

1. Visit: https://sraf.nd.edu/loughranmcdonald-master-dictionary/
2. Download: **"Loughran-McDonald Master Dictionary w/ Sentiment Word Lists"**
3. Extract the ZIP file
4. Locate the file: `Loughran-McDonald_MasterDictionary_1993-2024.csv`

### 2. Place in Data Directory

Copy `Loughran-McDonald_MasterDictionary_1993-2024.csv` to your `data/` directory (at the project root level):

```
ai-enthusiasm-research/
├── src/
│   └── data_analysis/
│       ├── analysisAI_LM.py
│       └── covid_sentiment_LM.py
├── data/
│   ├── Loughran-McDonald_MasterDictionary_1993-2024.csv  ← Place here
│   ├── processed/
│   └── raw/
└── ...
```

### 3. Run Analysis

The scripts can be run from anywhere in your project. Both approaches work:

**From project root:**
```bash
python3 src/data_analysis/analysisAI_LM.py
python3 src/data_analysis/covid_sentiment_LM.py
```

**From script directory:**
```bash
cd src/data_analysis
python3 analysisAI_LM.py
python3 covid_sentiment_LM.py
```

## About the Loughran-McDonald Dictionary

The Loughran-McDonald Master Dictionary is the standard sentiment lexicon for financial text analysis.

**Reference:**
> Loughran, T., & McDonald, B. (2011). When is a liability not a liability? Textual analysis, dictionaries, and 10-Ks. *The Journal of Finance*, 66(1), 35-65.

**Dictionary Contents:**
- **Positive:** 354 words indicating favorable sentiment
- **Negative:** 2,355 words indicating adverse sentiment  
- **Uncertainty:** 297 words indicating ambiguity or risk

**Why LM for Business Text?**
- Designed specifically for financial/business context
- Avoids false positives common in general sentiment dictionaries
- Widely used in academic research (5,000+ citations)
- Provides reproducible, interpretable results

## Troubleshooting

If the dictionary file is missing when you run the scripts, you'll see:

```
ERROR: Loughran-McDonald dictionary not found at data/Loughran-McDonald_MasterDictionary_1993-2024.csv
```

Follow the download instructions above to resolve this.