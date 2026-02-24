"""
Data transformation module for anime ETL pipeline.
Handles normalization, fuzzy matching, deduplication, and aggregations.
"""

import re
import unicodedata
from typing import Any, Dict, List

import pandas as pd
from rapidfuzz import fuzz


# --- TASK 1: Normalize titles ---
def normalize_title(title: str) -> str:
    """
    Normalize anime title for fuzzy matching.

    Normalization steps:
    1. Lowercase
    2. Remove accents (é → e)
    3. Remove punctuation
    4. Remove extra spaces
    5. Strip whitespace

    Args:
        title: Raw anime title

    Returns:
        Normalized title

    Examples:
        >>> normalize_title("Fullmetal Alchemist: Brotherhood")
        'fullmetal alchemist brotherhood'
        >>> normalize_title("Shingeki no Kyojin!")
        'shingeki no kyojin'
    """

    # --- Handling lower and accents ---
    # All titles in lower
    title = title.lower()

    # Remove accents
    # NFD = Normalization Form Decomposed (divide é in e + accent)
    title = unicodedata.normalize("NFD", title)
    # Keep only unaccented characters
    title = "".join(char for char in title if unicodedata.category(char) != "Mn")

    # --- Handling punctuation ---
    # Replaces all non-alphanumeric characters with a space
    title = re.sub(r"[^a-z0-9\s]", " ", title)

    # --- Handling multiple spaces + trim ---
    # Replace multiple spaces with a single space
    title = re.sub(r"\s+", " ", title)

    # Remove leading/trailing spaces
    title = title.strip()

    return title


# --- TASK 2: Match titles with fuzzymatching ---
def fuzzy_match_titles(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    title_col1: str,
    title_col2: str,
    threshold: int = 85,
) -> pd.DataFrame:
    """
    Find fuzzy matches between two DataFrames based on title similarity.

    Uses RapidFuzz to compute similarity scores between normalized titles.
    Returns ALL matches with score >= threshold (not just the best match).

    Args:
        df1: First DataFrame (e.g., Kaggle data)
        df2: Second DataFrame (e.g., Jikan data)
        title_col1: Column name for titles in df1 (e.g., 'name')
        title_col2: Column name for titles in df2 (e.g., 'title')
        threshold: Minimum similarity score (0-100) to consider a match (default: 85)

    Returns:
        DataFrame with columns:
            - index_df1: Index from df1
            - index_df2: Index from df2
            - title_df1: Original title from df1
            - title_df2: Matched title from df2
            - score: Similarity score (0-100)

    Examples:
        >>> df_kaggle = pd.DataFrame({'name': ['Cowboy Bebop', 'Naruto']})
        >>> df_jikan = pd.DataFrame({'title': ['Cowboy Bebop', 'One Piece']})
        >>> matches = fuzzy_match_titles(df_kaggle, df_jikan, 'name', 'title')
        >>> len(matches)
        1  # Only 'Cowboy Bebop' matches with score 100

    Notes:
        - Titles are normalized before comparison (lowercase, no accents, no punctuation)
        - Uses RapidFuzz fuzz.ratio() for similarity scoring
        - Returns ALL matches >= threshold, not just best match
        - Time complexity: O(n × m) where n=len(df1), m=len(df2)
    """
    matches = []

    for idx1, row1 in df1.iterrows():
        title1 = row1[title_col1]
        title1_normalized = normalize_title(title1)

        for idx2, row2 in df2.iterrows():
            title2 = row2[title_col2]
            title2_normalized = normalize_title(title2)

            score = fuzz.ratio(title1_normalized, title2_normalized)

            # Add ALL matches above threshold (not just best)
            if score >= threshold:
                matches.append(
                    {
                        "index_df1": idx1,
                        "index_df2": idx2,
                        "title_df1": title1,
                        "title_df2": title2,
                        "score": score,
                    }
                )

    return pd.DataFrame(matches)


def deduplicate_animes(
    df: pd.DataFrame, title_col: str, threshold: int = 90
) -> pd.DataFrame:
    """
    Remove duplicate animes based on fuzzy title matching.

    Args:
        df: DataFrame with anime data
        title_col: Column name containing titles
        threshold: Similarity threshold for duplicates (default: 90)

    Returns:
        DataFrame with duplicates removed (keeps first occurrence)

    Examples:
        >>> df = pd.DataFrame({'name': ['Cowboy Bebop', 'Cowboy Bepop', 'Naruto']})
        >>> df_clean = deduplicate_animes(df, 'name', threshold=90)
        >>> len(df_clean)
        2  # 'Cowboy Bepop' removed as duplicate

    Notes:
        - Compares DataFrame with itself using fuzzy matching
        - Self-matches (same index) are excluded
        - When duplicates found, keeps the entry with the lowest index
    """
    # Compare df with itself to find potential duplicates
    matches = fuzzy_match_titles(df, df, title_col, title_col, threshold)

    # Remove self-matches (anime matching with itself)
    matches = matches[matches["index_df1"] != matches["index_df2"]]

    # Identify indices to drop
    # For each pair (A, B), we drop B (the higher index)
    indices_to_drop = set()
    for _, match in matches.iterrows():
        idx1 = match["index_df1"]
        idx2 = match["index_df2"]
        # Keep the smaller index, drop the larger one
        indices_to_drop.add(max(idx1, idx2))

    return df.drop(index=list(indices_to_drop)).reset_index(drop=True)


# --- TASK 3: Convert Jikan & AniList data to df ---
def convert_jikan_to_dataframe(jikan_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convert Jikan API list of dicts to DataFrame.

    Args:
        jikan_data: List of anime dicts from extract_jikan_api()

    Returns:
        DataFrame with normalized columns
    """
    if not jikan_data:
        return pd.DataFrame()

    # Convert to DataFrame
    df = pd.DataFrame(jikan_data)

    # Select/rename relevant columns
    # Jikan structure: {'mal_id': 1, 'title': '...', 'synopsis': '...', 'studios': [...]}
    return df


def convert_anilist_to_dataframe(anilist_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert AniList GraphQL response to DataFrame.

    Args:
        anilist_data: Response from extract_anilist_graphql()

    Returns:
        DataFrame with normalized columns
    """
    # AniList structure: {'data': {'Page': {'media': [...]}}}
    media_list = anilist_data.get("data", {}).get("Page", {}).get("media", [])

    if not media_list:
        return pd.DataFrame()

    return pd.DataFrame(media_list)
