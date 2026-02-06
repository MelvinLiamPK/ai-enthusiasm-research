"""
LinkedIn URL Name Verification Module
=====================================
Provides deterministic, replicable verification of LinkedIn profile URLs
by checking if the director's name appears in the LinkedIn profile title.

This module is used by find_linkedin_urls_sp500.py to filter out wrong profiles.

Usage:
    from linkedin_verification import verify_name_match, verify_url_data, extract_name_parts
"""

import re
import pandas as pd


def extract_name_parts(director_name):
    """
    Extract first name, last name, and nickname variations from director name.
    
    Args:
        director_name: Full name like "Timothy D. Cook" or "Robert A. Iger"
    
    Returns:
        dict with 'first_names' (list), 'last_names' (list) for matching
    """
    if pd.isna(director_name):
        return {'first_names': [], 'last_names': []}
    
    # Clean the name
    name = str(director_name).strip()
    
    # Remove credentials and titles
    name = re.sub(r'\b(Ph\.?D\.?|M\.?D\.?|MBA|M\.?B\.?A\.?|CPA|C\.?P\.?A\.?|J\.?D\.?|Esq\.?|B\.?A\.?|B\.?S\.?|M\.?S\.?|M\.?P\.?H\.?|KBE|AC|OBE|CBE)\b', '', name, flags=re.IGNORECASE)
    
    # Remove extra dots and clean up
    name = re.sub(r'\.+', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    
    # Split into parts
    parts = [p.strip() for p in name.split() if p.strip()]
    
    if not parts:
        return {'first_names': [], 'last_names': []}
    
    # Identify suffixes to exclude from last name
    suffixes = {'jr', 'sr', 'ii', 'iii', 'iv', 'v', 'vi', 'vii', 'viii', '2nd', '3rd', '4th'}
    
    # First name is always first part
    first_name = parts[0].lower()
    
    # Build list of first name variations (nicknames)
    first_names = [first_name]
    
    # Common nickname mappings
    nicknames = {
        'robert': ['bob', 'rob', 'bobby', 'bert'],
        'william': ['bill', 'will', 'billy', 'willy', 'liam'],
        'richard': ['rick', 'dick', 'rich', 'ricky'],
        'james': ['jim', 'jimmy', 'jamie'],
        'timothy': ['tim', 'timmy'],
        'thomas': ['tom', 'tommy'],
        'michael': ['mike', 'mick', 'mickey'],
        'joseph': ['joe', 'joey'],
        'christopher': ['chris', 'kit'],
        'anthony': ['tony', 'ant'],
        'steven': ['steve', 'stevie'],
        'stephen': ['steve', 'stevie'],
        'edward': ['ed', 'eddie', 'ted', 'teddy'],
        'charles': ['charlie', 'chuck', 'chas'],
        'daniel': ['dan', 'danny'],
        'matthew': ['matt', 'matty'],
        'andrew': ['andy', 'drew'],
        'david': ['dave', 'davey'],
        'kenneth': ['ken', 'kenny'],
        'ronald': ['ron', 'ronny', 'ronnie'],
        'donald': ['don', 'donny', 'donnie'],
        'raymond': ['ray'],
        'lawrence': ['larry', 'lars'],
        'nicholas': ['nick', 'nicky'],
        'benjamin': ['ben', 'benny', 'benji'],
        'samuel': ['sam', 'sammy'],
        'gregory': ['greg', 'gregg'],
        'patrick': ['pat', 'paddy'],
        'alexander': ['alex', 'al', 'xander'],
        'albert': ['al', 'bert', 'bertie'],
        'frederick': ['fred', 'freddy', 'freddie'],
        'gerald': ['jerry', 'gerry'],
        'harold': ['harry', 'hal'],
        'jeffrey': ['jeff', 'geoff'],
        'jonathan': ['jon', 'john', 'jonny'],
        'peter': ['pete'],
        'phillip': ['phil'],
        'philip': ['phil'],
        'stanley': ['stan'],
        'theodore': ['ted', 'teddy', 'theo'],
        'walter': ['walt', 'wally'],
        'elizabeth': ['liz', 'lizzy', 'beth', 'betty', 'eliza'],
        'margaret': ['maggie', 'meg', 'peggy', 'marge'],
        'catherine': ['cathy', 'kate', 'katie', 'cat'],
        'katherine': ['kathy', 'kate', 'katie', 'kat'],
        'patricia': ['pat', 'patty', 'trish'],
        'jennifer': ['jen', 'jenny'],
        'jessica': ['jess', 'jessie'],
        'susan': ['sue', 'susie', 'suzy'],
        'rebecca': ['becky', 'becca'],
        'barbara': ['barb', 'barbie', 'babs'],
        'dorothy': ['dot', 'dotty', 'dottie'],
        'deborah': ['deb', 'debbie'],
        'nancy': ['nan'],
        'carolyn': ['carol', 'carrie'],
        'christine': ['chris', 'christy', 'tina'],
        'virginia': ['ginny', 'ginger'],
        'jacqueline': ['jackie', 'jacqui'],
        'millard': ['mickey'],  # Millard "Mickey" Drexler
    }
    
    if first_name in nicknames:
        first_names.extend(nicknames[first_name])
    
    # Also check reverse (if we have a nickname, add formal name)
    for formal, nicks in nicknames.items():
        if first_name in nicks and formal not in first_names:
            first_names.append(formal)
    
    # Last name - skip suffixes
    last_names = []
    for i in range(len(parts) - 1, 0, -1):
        part = parts[i].lower()
        if part not in suffixes and len(part) > 1:
            last_names.append(part)
            break
    
    # For compound last names (de Rothschild, Van Dyke, etc.), also try the last word
    if len(parts) > 2:
        last_part = parts[-1].lower()
        if last_part not in suffixes and last_part not in last_names and len(last_part) > 1:
            last_names.append(last_part)
    
    return {
        'first_names': first_names,
        'last_names': last_names
    }


def clean_company_name_for_matching(company_name):
    """
    Extract key company words for matching, removing common suffixes and noise.
    
    Args:
        company_name: Company name like "Apple Inc." or "The Coca-Cola Company"
    
    Returns:
        list of significant words to match (lowercase)
    """
    if pd.isna(company_name):
        return []
    
    # Remove common suffixes
    name = str(company_name).lower()
    suffixes = [
        r'\s*,?\s*inc\.?\s*$',
        r'\s*,?\s*corp\.?\s*$',
        r'\s*,?\s*corporation\s*$',
        r'\s*,?\s*ltd\.?\s*$',
        r'\s*,?\s*llc\s*$',
        r'\s*,?\s*l\.l\.c\.?\s*$',
        r'\s*,?\s*plc\s*$',
        r'\s*,?\s*co\.?\s*$',
        r'\s*,?\s*company\s*$',
        r'\s*,?\s*limited\s*$',
        r'\s*,?\s*group\s*$',
    ]
    
    for suffix in suffixes:
        name = re.sub(suffix, '', name, flags=re.IGNORECASE)
    
    # Clean up and split
    name = re.sub(r'[^\w\s-]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    
    # Split into words and filter
    words = name.split()
    
    # Remove common noise words
    noise_words = {'the', 'a', 'an', 'and', 'or', 'of', 'in', 'at', 'by', 'for', 'on'}
    
    # Keep words that are:
    # 1. Length > 3 (skip "Inc", "Ltd", "Co") OR
    # 2. Length 2-3 AND all uppercase (like "BXP", "RTX", "KLA")
    # 3. Not noise words
    # 4. Not just numbers
    significant_words = []
    for w in words:
        if w in noise_words or w.isdigit():
            continue
        # Keep longer words OR short uppercase acronyms
        if len(w) > 3 or (len(w) >= 2 and w.upper() == company_name.strip()):
            significant_words.append(w)
    
    # Special case: if NO significant words found AND original name is short (2-4 chars)
    # then use the original name as-is (for BXP, KLA, RTX, etc.)
    if not significant_words and 2 <= len(company_name.strip()) <= 4:
        significant_words = [company_name.strip().lower()]
    
    return significant_words


def verify_name_match(director_name, linkedin_title):
    """
    Verify if the LinkedIn profile title matches the director's name.
    
    This is a deterministic, replicable verification method.
    
    Args:
        director_name: Director's name (e.g., "Timothy D. Cook")
        linkedin_title: LinkedIn result title (e.g., "Tim Cook - Apple CEO | LinkedIn")
    
    Returns:
        dict with:
            - 'verified': bool - True if name matches
            - 'match_type': str - 'first_name', 'last_name', 'both', or 'none'
            - 'matched_first': str or None - which first name matched
            - 'matched_last': str or None - which last name matched
    """
    if pd.isna(director_name) or pd.isna(linkedin_title):
        return {
            'verified': False,
            'match_type': 'none',
            'matched_first': None,
            'matched_last': None
        }
    
    # Get name parts
    name_parts = extract_name_parts(director_name)
    first_names = name_parts['first_names']
    last_names = name_parts['last_names']
    
    # Clean and lowercase the title
    title_lower = str(linkedin_title).lower()
    
    # Check for matches
    matched_first = None
    matched_last = None
    
    for fn in first_names:
        # Use word boundary matching to avoid partial matches
        # e.g., "Tim" should match "Tim Cook" but not "Optimization"
        if re.search(r'\b' + re.escape(fn) + r'\b', title_lower):
            matched_first = fn
            break
    
    for ln in last_names:
        if re.search(r'\b' + re.escape(ln) + r'\b', title_lower):
            matched_last = ln
            break
    
    # Determine match type and verification status
    if matched_first and matched_last:
        match_type = 'both'
        verified = True
    elif matched_first:
        match_type = 'first_name'
        verified = True
    elif matched_last:
        match_type = 'last_name'
        verified = True
    else:
        match_type = 'none'
        verified = False
    
    return {
        'verified': verified,
        'match_type': match_type,
        'matched_first': matched_first,
        'matched_last': matched_last
    }


def verify_company_match(company_name, linkedin_title):
    """
    Verify if the company name appears in the LinkedIn profile title.
    
    Args:
        company_name: Company name (e.g., "Apple Inc.")
        linkedin_title: LinkedIn result title (e.g., "Tim Cook - Apple CEO | LinkedIn")
    
    Returns:
        dict with:
            - 'company_matched': bool - True if company appears in title
            - 'matched_words': list - which company words matched
    """
    if pd.isna(company_name) or pd.isna(linkedin_title):
        return {
            'company_matched': False,
            'matched_words': []
        }
    
    company_words = clean_company_name_for_matching(company_name)
    
    if not company_words:
        return {
            'company_matched': False,
            'matched_words': []
        }
    
    title_lower = str(linkedin_title).lower()
    matched_words = []
    
    for word in company_words:
        if re.search(r'\b' + re.escape(word) + r'\b', title_lower):
            matched_words.append(word)
    
    # Company matches if ANY significant word appears
    # (some companies have multiple words, we don't need all of them)
    company_matched = len(matched_words) > 0
    
    return {
        'company_matched': company_matched,
        'matched_words': matched_words
    }


def check_board_role_keywords(linkedin_title):
    """
    Check if LinkedIn title contains board/director role keywords.
    
    Args:
        linkedin_title: LinkedIn profile title
    
    Returns:
        dict with:
            - 'has_board_keyword': bool
            - 'matched_keywords': list of matched keywords
    """
    if pd.isna(linkedin_title):
        return {
            'has_board_keyword': False,
            'matched_keywords': []
        }
    
    title_lower = str(linkedin_title).lower()
    
    # Board-related keywords
    board_keywords = [
        'board member',
        'board of directors',
        'board director',
        'independent director',
        'non-executive director',
        'outside director',
        'board',
        'director',
        'trustee',
        'advisory board',
        'advisory council',
        'governance',
        'chairman',
        'chairwoman',
        'chairperson',
        'vice chair',
        'lead director',
        'presiding director'
    ]
    
    matched = []
    for keyword in board_keywords:
        if keyword in title_lower:
            matched.append(keyword)
    
    return {
        'has_board_keyword': len(matched) > 0,
        'matched_keywords': matched
    }


def verify_name_and_company_match(director_name, company_name, linkedin_title):
    """
    Comprehensive verification with numeric match score.
    
    Match Score System:
        100: Perfect match (both first+last name AND company)
         90: Full name match (first+last) but no company
         70: Partial name match (first OR last) AND company
         60: Partial name match (first OR last) but no company
         30: Company match but no name match (WRONG PERSON)
          0: No matches at all
    
    Args:
        director_name: Director's full name
        company_name: Company name
        linkedin_title: LinkedIn profile title from search result
    
    Returns:
        dict with:
            - 'match_score': int (0-100) - numeric score indicating match quality
            - 'verified': bool - True if match_score >= 70 (name + company)
            - 'name_matched': bool - True if any name part matched
            - 'company_matched': bool - True if company matched
            - 'match_type': str - description of match type
            - 'matched_first': str or None
            - 'matched_last': str or None
            - 'matched_company_words': list
            - 'quality_flag': str - 'EXCELLENT', 'GOOD', 'WEAK', 'WRONG_PERSON', 'NO_MATCH'
    """
    # Get name matching results
    name_result = verify_name_match(director_name, linkedin_title)
    
    # Get company matching results
    company_result = verify_company_match(company_name, linkedin_title)
    
    # Determine if we have first name, last name matches
    has_first = name_result['matched_first'] is not None
    has_last = name_result['matched_last'] is not None
    has_company = company_result['company_matched']
    
    # Calculate match score
    if has_first and has_last and has_company:
        match_score = 100
        quality_flag = 'EXCELLENT'
        match_type = 'full_name_and_company'
    elif has_first and has_last and not has_company:
        match_score = 90
        quality_flag = 'GOOD'
        match_type = 'full_name_no_company'
    elif (has_first or has_last) and has_company:
        match_score = 70
        quality_flag = 'GOOD'
        match_type = 'partial_name_with_company'
    elif (has_first or has_last) and not has_company:
        match_score = 60
        quality_flag = 'WEAK'
        match_type = 'partial_name_no_company'
    elif not (has_first or has_last) and has_company:
        match_score = 30
        quality_flag = 'WRONG_PERSON'
        match_type = 'company_only_no_name'
    else:
        match_score = 0
        quality_flag = 'NO_MATCH'
        match_type = 'no_match'
    
    # Verification threshold: require name match AND company match
    # This means match_score >= 70 (at least partial name + company)
    verified = match_score >= 70
    
    return {
        'match_score': match_score,
        'verified': verified,
        'name_matched': has_first or has_last,
        'company_matched': has_company,
        'match_type': match_type,
        'quality_flag': quality_flag,
        'matched_first': name_result['matched_first'],
        'matched_last': name_result['matched_last'],
        'matched_company_words': company_result['matched_words']
    }


def verify_director_match(director_name, company_name, linkedin_title):
    """
    Verify LinkedIn profile match for BOARD DIRECTORS specifically.
    
    Directors typically work at other companies (employment) while serving on boards.
    Their LinkedIn titles show their employment, NOT their board seats.
    
    Therefore, this function:
    1. Prioritizes NAME matching (primary signal)
    2. Checks for board role keywords (bonus signal)
    3. Optionally checks company (rare but highest confidence)
    
    Match Score System for Directors:
        100: Full name (first+last) + Board keyword + Company
             (Rare - director lists board seat in title with company name)
        
         95: Full name (first+last) + Board keyword
             (Strong - name matches and title mentions board role)
        
         90: Full name (first+last), no board keyword, no company
             (Good - name matches, typical director profile showing employment)
        
         85: Partial name (first OR last) + Board keyword + Company
             (Good - partial name but board role and company confirm)
        
         80: Partial name (first OR last) + Board keyword
             (Fair - partial name but board keyword provides confidence)
        
         70: Full name + Company, no board keyword
             (Fair - might be employee listing, not board seat)
        
         60: Partial name (first OR last), no board keyword, no company
             (Weak - common names have high false positive risk)
        
         30: Company match only, no name match
             (Wrong person - definitely incorrect)
        
          0: No matches at all
    
    Args:
        director_name: Director's full name
        company_name: Company where they serve as director
        linkedin_title: LinkedIn profile title from search result
    
    Returns:
        dict with match_score and detailed match information
    """
    # Get name matching results
    name_result = verify_name_match(director_name, linkedin_title)
    
    # Get company matching results (optional for directors)
    company_result = verify_company_match(company_name, linkedin_title)
    
    # Get board role keywords
    board_result = check_board_role_keywords(linkedin_title)
    
    # Determine match components
    has_first = name_result['matched_first'] is not None
    has_last = name_result['matched_last'] is not None
    has_company = company_result['company_matched']
    has_board_keyword = board_result['has_board_keyword']
    
    # Calculate match score for DIRECTORS
    if has_first and has_last and has_board_keyword and has_company:
        match_score = 100
        quality_flag = 'EXCELLENT'
        match_type = 'full_name_board_keyword_company'
    elif has_first and has_last and has_board_keyword:
        match_score = 95
        quality_flag = 'EXCELLENT'
        match_type = 'full_name_with_board_keyword'
    elif has_first and has_last:
        match_score = 90
        quality_flag = 'GOOD'
        match_type = 'full_name_typical_director'
    elif (has_first or has_last) and has_board_keyword and has_company:
        match_score = 85
        quality_flag = 'GOOD'
        match_type = 'partial_name_board_keyword_company'
    elif (has_first or has_last) and has_board_keyword:
        match_score = 80
        quality_flag = 'GOOD'
        match_type = 'partial_name_with_board_keyword'
    elif has_first and has_last and has_company:
        match_score = 70
        quality_flag = 'FAIR'
        match_type = 'full_name_company_no_board'
    elif (has_first or has_last):
        match_score = 60
        quality_flag = 'WEAK'
        match_type = 'partial_name_only'
    elif has_company:
        match_score = 30
        quality_flag = 'WRONG_PERSON'
        match_type = 'company_only_no_name'
    else:
        match_score = 0
        quality_flag = 'NO_MATCH'
        match_type = 'no_match'
    
    # Verification threshold for directors: 80+ (name match with board keyword OR full name)
    # This is lower than general verification because directors don't show board seats in titles
    verified = match_score >= 80
    
    return {
        'match_score': match_score,
        'verified': verified,
        'name_matched': has_first or has_last,
        'company_matched': has_company,
        'board_keyword_matched': has_board_keyword,
        'match_type': match_type,
        'quality_flag': quality_flag,
        'matched_first': name_result['matched_first'],
        'matched_last': name_result['matched_last'],
        'matched_company_words': company_result['matched_words'],
        'matched_board_keywords': board_result['matched_keywords']
    }
    """
    Check if LinkedIn title contains board/director role keywords.
    
    Args:
        linkedin_title: LinkedIn profile title
    
    Returns:
        dict with:
            - 'has_board_keyword': bool
            - 'matched_keywords': list of matched keywords
    """
    if pd.isna(linkedin_title):
        return {
            'has_board_keyword': False,
            'matched_keywords': []
        }
    
    title_lower = str(linkedin_title).lower()
    
    # Board-related keywords
    board_keywords = [
        'board member',
        'board of directors',
        'board director',
        'independent director',
        'non-executive director',
        'outside director',
        'board',
        'director',
        'trustee',
        'advisory board',
        'advisory council',
        'governance',
        'chairman',
        'chairwoman',
        'chairperson',
        'vice chair',
        'lead director',
        'presiding director'
    ]
    
    matched = []
    for keyword in board_keywords:
        if keyword in title_lower:
            matched.append(keyword)
    
    return {
        'has_board_keyword': len(matched) > 0,
        'matched_keywords': matched
    }


def verify_director_match(director_name, company_name, linkedin_title):
    """
    Verify LinkedIn profile match for BOARD DIRECTORS specifically.
    
    Directors typically work at other companies (employment) while serving on boards.
    Their LinkedIn titles show their employment, NOT their board seats.
    
    Therefore, this function:
    1. Prioritizes NAME matching (primary signal)
    2. Checks for board role keywords (bonus signal)
    3. Optionally checks company (rare but highest confidence)
    
    Match Score System for Directors:
        100: Full name (first+last) + Board keyword + Company
             (Rare - director lists board seat in title with company name)
        
         95: Full name (first+last) + Board keyword
             (Strong - name matches and title mentions board role)
        
         90: Full name (first+last), no board keyword, no company
             (Good - name matches, typical director profile showing employment)
        
         85: Partial name (first OR last) + Board keyword + Company
             (Good - partial name but board role and company confirm)
        
         80: Partial name (first OR last) + Board keyword
             (Fair - partial name but board keyword provides confidence)
        
         70: Full name + Company, no board keyword
             (Fair - might be employee listing, not board seat)
        
         60: Partial name (first OR last), no board keyword, no company
             (Weak - common names have high false positive risk)
        
         30: Company match only, no name match
             (Wrong person - definitely incorrect)
        
          0: No matches at all
    
    Args:
        director_name: Director's full name
        company_name: Company where they serve as director
        linkedin_title: LinkedIn profile title from search result
    
    Returns:
        dict with match_score and detailed match information
    """
    # Get name matching results
    name_result = verify_name_match(director_name, linkedin_title)
    
    # Get company matching results (optional for directors)
    company_result = verify_company_match(company_name, linkedin_title)
    
    # Get board role keywords
    board_result = check_board_role_keywords(linkedin_title)
    
    # Determine match components
    has_first = name_result['matched_first'] is not None
    has_last = name_result['matched_last'] is not None
    has_company = company_result['company_matched']
    has_board_keyword = board_result['has_board_keyword']
    
    # Calculate match score for DIRECTORS
    if has_first and has_last and has_board_keyword and has_company:
        match_score = 100
        quality_flag = 'EXCELLENT'
        match_type = 'full_name_board_keyword_company'
    elif has_first and has_last and has_board_keyword:
        match_score = 95
        quality_flag = 'EXCELLENT'
        match_type = 'full_name_with_board_keyword'
    elif has_first and has_last:
        match_score = 90
        quality_flag = 'GOOD'
        match_type = 'full_name_typical_director'
    elif (has_first or has_last) and has_board_keyword and has_company:
        match_score = 85
        quality_flag = 'GOOD'
        match_type = 'partial_name_board_keyword_company'
    elif (has_first or has_last) and has_board_keyword:
        match_score = 80
        quality_flag = 'GOOD'
        match_type = 'partial_name_with_board_keyword'
    elif has_first and has_last and has_company:
        match_score = 70
        quality_flag = 'FAIR'
        match_type = 'full_name_company_no_board'
    elif (has_first or has_last):
        match_score = 60
        quality_flag = 'WEAK'
        match_type = 'partial_name_only'
    elif has_company:
        match_score = 30
        quality_flag = 'WRONG_PERSON'
        match_type = 'company_only_no_name'
    else:
        match_score = 0
        quality_flag = 'NO_MATCH'
        match_type = 'no_match'
    
    # Verification threshold for directors: 80+ (name match with board keyword OR full name)
    # This is lower than general verification because directors don't show board seats in titles
    verified = match_score >= 80
    
    return {
        'match_score': match_score,
        'verified': verified,
        'name_matched': has_first or has_last,
        'company_matched': has_company,
        'board_keyword_matched': has_board_keyword,
        'match_type': match_type,
        'quality_flag': quality_flag,
        'matched_first': name_result['matched_first'],
        'matched_last': name_result['matched_last'],
        'matched_company_words': company_result['matched_words'],
        'matched_board_keywords': board_result['matched_keywords']
    }
    """
    Comprehensive verification with numeric match score.
    
    Match Score System:
        100: Perfect match (both first+last name AND company)
         90: Full name match (first+last) but no company
         70: Partial name match (first OR last) AND company
         60: Partial name match (first OR last) but no company
         30: Company match but no name match (WRONG PERSON)
          0: No matches at all
    
    Args:
        director_name: Director's full name
        company_name: Company name
        linkedin_title: LinkedIn profile title from search result
    
    Returns:
        dict with:
            - 'match_score': int (0-100) - numeric score indicating match quality
            - 'verified': bool - True if match_score >= 70 (name + company)
            - 'name_matched': bool - True if any name part matched
            - 'company_matched': bool - True if company matched
            - 'match_type': str - description of match type
            - 'matched_first': str or None
            - 'matched_last': str or None
            - 'matched_company_words': list
            - 'quality_flag': str - 'EXCELLENT', 'GOOD', 'WEAK', 'WRONG_PERSON', 'NO_MATCH'
    """
    # Get name matching results
    name_result = verify_name_match(director_name, linkedin_title)
    
    # Get company matching results
    company_result = verify_company_match(company_name, linkedin_title)
    
    # Determine if we have first name, last name matches
    has_first = name_result['matched_first'] is not None
    has_last = name_result['matched_last'] is not None
    has_company = company_result['company_matched']
    
    # Calculate match score
    if has_first and has_last and has_company:
        match_score = 100
        quality_flag = 'EXCELLENT'
        match_type = 'full_name_and_company'
    elif has_first and has_last and not has_company:
        match_score = 90
        quality_flag = 'GOOD'
        match_type = 'full_name_no_company'
    elif (has_first or has_last) and has_company:
        match_score = 70
        quality_flag = 'GOOD'
        match_type = 'partial_name_with_company'
    elif (has_first or has_last) and not has_company:
        match_score = 60
        quality_flag = 'WEAK'
        match_type = 'partial_name_no_company'
    elif not (has_first or has_last) and has_company:
        match_score = 30
        quality_flag = 'WRONG_PERSON'
        match_type = 'company_only_no_name'
    else:
        match_score = 0
        quality_flag = 'NO_MATCH'
        match_type = 'no_match'
    
    # Verification threshold: require name match AND company match
    # This means match_score >= 70 (at least partial name + company)
    verified = match_score >= 70
    
    return {
        'match_score': match_score,
        'verified': verified,
        'name_matched': has_first or has_last,
        'company_matched': has_company,
        'match_type': match_type,
        'quality_flag': quality_flag,
        'matched_first': name_result['matched_first'],
        'matched_last': name_result['matched_last'],
        'matched_company_words': company_result['matched_words']
    }


def verify_url_data(df, apply_filter=False, min_match_score=70):
    """
    Verify all URLs in a DataFrame using comprehensive name+company matching.
    
    Args:
        df: DataFrame with 'director_name_clean', 'company_name_clean', and 'linkedin_title' columns
        apply_filter: If True, set linkedin_url to None for rows below min_match_score
        min_match_score: Minimum score to keep URLs (default 70 = partial name + company)
    
    Returns:
        DataFrame with verification columns added:
            - match_score: 0-100 numeric score
            - verified: bool (score >= min_match_score)
            - quality_flag: EXCELLENT/GOOD/WEAK/WRONG_PERSON/NO_MATCH
            - match_type: detailed match description
            - name_matched: bool
            - company_matched: bool
    """
    print("\n[Verification] Checking name+company matches in LinkedIn titles...")
    print(f"    Minimum match score for verification: {min_match_score}")
    
    # Add verification columns
    df['match_score'] = 0
    df['verified'] = False
    df['quality_flag'] = 'NO_MATCH'
    df['match_type'] = 'no_match'
    df['name_matched'] = False
    df['company_matched'] = False
    df['matched_name'] = None
    df['matched_company'] = None
    df['board_keyword_matched'] = False
    df['matched_board_keywords'] = None
    
    # Track counts by quality
    quality_counts = {
        'EXCELLENT': 0,  # 100: full name + company
        'GOOD': 0,       # 90 or 70: full/partial name + maybe company
        'WEAK': 0,       # 60: partial name, no company
        'WRONG_PERSON': 0,  # 30: company but wrong name
        'FAIR': 0,       # 70: full name + company, no board keyword
        'NO_MATCH': 0    # 0: nothing matched
    }
    no_url_count = 0
    
    for idx, row in df.iterrows():
        if pd.isna(row.get('linkedin_url')) or row.get('search_status') != 'found':
            no_url_count += 1
            continue
        
        # Get the verification result with match score
        result = verify_director_match(
            director_name=row.get('director_name_clean', row.get('director_name', '')),
            company_name=row.get('company_name_clean', row.get('company_name', '')),
            linkedin_title=row.get('linkedin_title', '')
        )
        
        # Store results
        df.at[idx, 'match_score'] = result['match_score']
        df.at[idx, 'verified'] = result['verified']
        df.at[idx, 'quality_flag'] = result['quality_flag']
        df.at[idx, 'match_type'] = result['match_type']
        df.at[idx, 'name_matched'] = result['name_matched']
        df.at[idx, 'company_matched'] = result['company_matched']
        df.at[idx, 'board_keyword_matched'] = result.get('board_keyword_matched', False)
        
        # Build matched name string
        if result['matched_first'] and result['matched_last']:
            df.at[idx, 'matched_name'] = f"{result['matched_first']} {result['matched_last']}"
        elif result['matched_first']:
            df.at[idx, 'matched_name'] = result['matched_first']
        elif result['matched_last']:
            df.at[idx, 'matched_name'] = result['matched_last']
        
        # Store matched company words
        if result['matched_company_words']:
            df.at[idx, 'matched_company'] = ', '.join(result['matched_company_words'])
        
        # Store matched board keywords
        if result.get('matched_board_keywords'):
            df.at[idx, 'matched_board_keywords'] = ', '.join(result['matched_board_keywords'])
        
        # Track quality counts
        quality_counts[result['quality_flag']] += 1
        
        # Apply filter if requested
        if apply_filter and result['match_score'] < min_match_score:
            df.at[idx, 'linkedin_url'] = None
            df.at[idx, 'search_status'] = 'filtered_low_score'
    
    # Print detailed summary
    total_with_urls = sum(quality_counts.values())
    
    print(f"\n    Results breakdown:")
    print(f"    {'='*60}")
    print(f"    {'Quality':<15} {'Score':<10} {'Count':<10} {'%':<10}")
    print(f"    {'-'*60}")
    
    print(f"    {'EXCELLENT':<15} {'100':<10} {quality_counts['EXCELLENT']:<10} {100*quality_counts['EXCELLENT']/total_with_urls if total_with_urls > 0 else 0:.1f}%")
    print(f"    {'GOOD':<15} {'70-90':<10} {quality_counts['GOOD']:<10} {100*quality_counts['GOOD']/total_with_urls if total_with_urls > 0 else 0:.1f}%")
    print(f"    {'WEAK':<15} {'60':<10} {quality_counts['WEAK']:<10} {100*quality_counts['WEAK']/total_with_urls if total_with_urls > 0 else 0:.1f}%")
    print(f"    {'FAIR':<15} {'70':<10} {quality_counts['FAIR']:<10} {100*quality_counts['FAIR']/total_with_urls if total_with_urls > 0 else 0:.1f}%")
    print(f"    {'WRONG_PERSON':<15} {'30':<10} {quality_counts['WRONG_PERSON']:<10} {100*quality_counts['WRONG_PERSON']/total_with_urls if total_with_urls > 0 else 0:.1f}%")
    print(f"    {'NO_MATCH':<15} {'0':<10} {quality_counts['NO_MATCH']:<10} {100*quality_counts['NO_MATCH']/total_with_urls if total_with_urls > 0 else 0:.1f}%")
    print(f"    {'-'*60}")
    print(f"    {'Total URLs':<15} {'':<10} {total_with_urls:<10} {'100.0%':<10}")
    print(f"    {'No URL found':<15} {'':<10} {no_url_count:<10} {'':<10}")
    
    verified_count = (df['verified'] == True).sum()
    print(f"\n    Ã¢Å“â€œ Verified (score >= {min_match_score}): {verified_count:,} ({100*verified_count/total_with_urls if total_with_urls > 0 else 0:.1f}%)")
    
    if apply_filter:
        filtered_count = quality_counts['WEAK'] + quality_counts['WRONG_PERSON'] + quality_counts['NO_MATCH']
        print(f"    Ã°Å¸Å¡Â« Filtered out (score < {min_match_score}): {filtered_count:,}")
    
    return df



# =========================
# Standalone verification script
# =========================

def run_verification_standalone(input_file, output_file=None, apply_filter=False):
    """
    Run verification on a CSV file from command line.
    
    Args:
        input_file: Path to CSV with linkedin_url and director_name columns
        output_file: Path for output (default: adds _verified suffix)
        apply_filter: If True, null out unverified URLs
    """
    print("\n" + "=" * 60)
    print("LinkedIn URL Verification")
    print("=" * 60)
    
    # Load data
    print(f"\nLoading: {input_file}")
    df = pd.read_csv(input_file)
    print(f"Total records: {len(df):,}")
    
    # Run verification
    df = verify_url_data(df, apply_filter=apply_filter)
    
    # Determine output path
    if output_file is None:
        base, ext = os.path.splitext(input_file)
        output_file = f"{base}_verified{ext}"
    
    # Save results
    df.to_csv(output_file, index=False)
    print(f"\nÃƒÂ¢Ã…â€œÃ¢â‚¬Å“ Saved to: {output_file}")
    
    # Summary stats
    print(f"\n" + "=" * 60)
    print("VERIFICATION SUMMARY")
    print("=" * 60)
    
    total = len(df)
    urls_found = df['linkedin_url'].notna().sum()
    verified = df['verified'].sum()
    
    print(f"Total directors: {total:,}")
    print(f"URLs found: {urls_found:,} ({100*urls_found/total:.1f}%)")
    if urls_found > 0:
        print(f"Verified URLs: {verified:,} ({100*verified/total:.1f}% of total, {100*verified/urls_found:.1f}% of found)")
    
    return df


if __name__ == "__main__":
    import argparse
    import os
    
    parser = argparse.ArgumentParser(description='Verify LinkedIn URLs by name matching')
    parser.add_argument('input_file', help='CSV file with LinkedIn URLs to verify')
    parser.add_argument('--output', '-o', help='Output file path')
    parser.add_argument('--apply', action='store_true', help='Null out unverified URLs')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_file):
        print(f"ÃƒÂ¢Ã‚ÂÃ…â€™ File not found: {args.input_file}")
        sys.exit(1)
    
    run_verification_standalone(args.input_file, args.output, args.apply)