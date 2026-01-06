# AI Enthusiasm Research

Academic research project measuring "AI enthusiasm" among corporate leadership by analyzing social media engagement patterns of company directors and CEOs.

## Project Overview

This project examines how corporate leadership engages with AI-related topics on social media platforms, primarily LinkedIn. The research involves:

1. **Building a corporate leadership database** from WRDS (Wharton Research Data Services)
2. **Enriching with LinkedIn profile URLs** using profile discovery tools
3. **Collecting social media posts** from identified executives
4. **Analyzing AI-related content** through keyword matching and sentiment analysis
5. **Calculating AI enthusiasm metrics** for correlation with business outcomes

## Data Sources

- **WRDS execcomp.directorcomp**: Director and CEO compensation/identification data
- **LinkedIn**: Executive social media posts (collected via approved methods)

## Project Structure

```
ai-enthusiasm-research/
├── config/                 # Configuration templates
├── data/
│   ├── raw/               # Original WRDS exports (gitignored)
│   ├── processed/         # Cleaned datasets (gitignored)
│   └── samples/           # Small test files
├── notebooks/             # Exploratory analysis
├── src/
│   ├── data_collection/   # WRDS queries, scraping scripts
│   ├── data_processing/   # Cleaning and merging
│   └── analysis/          # Metrics and analysis
├── outputs/               # Results and figures
└── docs/                  # Documentation
```

## Setup

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/ai-enthusiasm-research.git
cd ai-enthusiasm-research
```

### 2. Create virtual environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure credentials
```bash
cp config/config_template.py config/config.py
# Edit config/config.py with your actual credentials
```

### 5. Set up WRDS access
Create a `.pgpass` file for WRDS authentication (see WRDS documentation).

## Usage

[To be updated as project develops]

## Data Notes

- Raw data files are not committed to the repository due to size
- Sample data files in `data/samples/` can be used for testing
- Full datasets available upon request for replication

## Author

[Your Name]

## License

[Choose appropriate license for academic research]
