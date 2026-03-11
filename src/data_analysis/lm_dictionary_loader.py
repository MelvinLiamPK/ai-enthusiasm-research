"""
Loughran-McDonald Dictionary Loader
====================================
Loads the full Loughran-McDonald Master Dictionary for sentiment analysis.

Download the dictionary from: https://sraf.nd.edu/loughranmcdonald-master-dictionary/
Place 'LM_MasterDictionary_1993-2021.csv' in the project root directory.
"""

import pandas as pd
import os
import sys

def load_lm_dictionary(dict_path=None):
    """
    Load the full Loughran-McDonald Master Dictionary.
    
    Args:
        dict_path: Optional path to the LM Master Dictionary CSV file.
                   If None, will auto-detect based on script location.
        
    Returns:
        tuple: (positive_words, negative_words, uncertainty_words) as sets
    """
    import os
    from pathlib import Path
    
    # If no path provided, auto-detect relative to this script
    if dict_path is None:
        script_dir = Path(__file__).parent.resolve()
        project_root = script_dir.parent.parent
        dict_path = project_root / 'data' / 'Loughran-McDonald_MasterDictionary_1993-2024.csv'
    else:
        dict_path = Path(dict_path)
    
    if not dict_path.exists():
        print(f"\nERROR: Loughran-McDonald dictionary not found at {dict_path}")
        print("\nPlease download the dictionary:")
        print("1. Go to: https://sraf.nd.edu/loughranmcdonald-master-dictionary/")
        print("2. Download 'Loughran-McDonald Master Dictionary w/ Sentiment Word Lists'")
        print("3. Extract and place 'Loughran-McDonald_MasterDictionary_1993-2024.csv' in the data/ directory (project root)")
        print("\nThe file should be at: data/Loughran-McDonald_MasterDictionary_1993-2024.csv")
        sys.exit(1)
    
    print(f"Loading LM dictionary from: {dict_path}")
    
    # Load the dictionary
    lm_dict = pd.read_csv(str(dict_path))
    
    # Extract word sets (convert to lowercase for case-insensitive matching)
    # The CSV has columns like 'Positive', 'Negative', 'Uncertainty' with values > 0 indicating membership
    positive_words = set(lm_dict[lm_dict['Positive'] > 0]['Word'].str.lower())
    negative_words = set(lm_dict[lm_dict['Negative'] > 0]['Word'].str.lower())
    uncertainty_words = set(lm_dict[lm_dict['Uncertainty'] > 0]['Word'].str.lower())
    
    print(f"  ✓ Loaded {len(positive_words)} positive words")
    print(f"  ✓ Loaded {len(negative_words)} negative words")
    print(f"  ✓ Loaded {len(uncertainty_words)} uncertainty words")
    
    return positive_words, negative_words, uncertainty_words


if __name__ == "__main__":
    # Test the loader
    print("Testing LM Dictionary Loader")
    print("=" * 60)
    
    pos, neg, unc = load_lm_dictionary()
    
    print("\n" + "=" * 60)
    print("Sample words:")
    print(f"  Positive (first 10): {list(pos)[:10]}")
    print(f"  Negative (first 10): {list(neg)[:10]}")
    print(f"  Uncertainty (first 10): {list(unc)[:10]}")
    print("=" * 60)
    print("\n✓ Dictionary loaded successfully!")