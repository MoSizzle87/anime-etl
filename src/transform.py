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
    Convert Jikan API response to DataFrame.

    Transforms list of anime dicts from Jikan API into a clean DataFrame.
    Nested fields (studios, genres) are flattened into comma-separated strings.

    Args:
        jikan_data: List of anime dicts from extract_jikan_api()

    Returns:
        DataFrame with columns: mal_id, title, synopsis, score, scored_by, studios, genres
        Empty DataFrame if input is empty

    Notes:
        - Studios list is converted to comma-separated string of studio names
        - Genres list is converted to comma-separated string of genre names
        - Only essential columns are kept in the output DataFrame
    """
    # Return empty DataFrame if no data
    if not jikan_data:
        return pd.DataFrame()

    # Convert list of dicts to DataFrame
    df = pd.DataFrame(jikan_data)

    # Flatten studios: [{"name": "Sunrise"}] → "Sunrise"
    df["studios"] = df["studios"].apply(
        lambda studios_list: (
            ", ".join([s["name"] for s in studios_list]) if studios_list else ""
        )
    )

    # Flatten genres: [{"name": "Action"}, {"name": "Sci-Fi"}] → "Action, Sci-Fi"
    df["genres"] = df["genres"].apply(
        lambda genres_list: (
            ", ".join([g["name"] for g in genres_list]) if genres_list else ""
        )
    )

    # Select and return essential columns only
    columns_to_keep = [
        "mal_id",
        "title",
        "synopsis",
        "score",
        "scored_by",
        "studios",
        "genres",
    ]
    return df[columns_to_keep]


def convert_anilist_to_dataframe(anilist_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert AniList GraphQL response to DataFrame.

    Extracts anime data from nested AniList GraphQL response structure
    and flattens the title field into separate romaji/english columns.

    Args:
        anilist_data: Response dict from extract_anilist_graphql()
                      Expected structure: {'data': {'Page': {'media': [...]}}}

    Returns:
        DataFrame with columns: id, idMal, title_romaji, title_english, averageScore, trending
        Empty DataFrame if no media found in response

    Notes:
        - AniList uses nested 'title' object with romaji/english fields
        - idMal links to MyAnimeList ID for cross-referencing
        - averageScore is AniList's community score (0-100)
    """
    # Extract media list from nested structure
    # AniList structure: {'data': {'Page': {'media': [...]}}}
    media_list = anilist_data.get("data", {}).get("Page", {}).get("media", [])

    # Return empty DataFrame if no data
    if not media_list:
        return pd.DataFrame()

    # Convert list of anime dicts to DataFrame
    df = pd.DataFrame(media_list)

    # Flatten title object: {"romaji": "...", "english": "..."} → separate columns
    df["title_romaji"] = df["title"].apply(lambda t: t.get("romaji", "") if t else "")
    df["title_english"] = df["title"].apply(lambda t: t.get("english", "") if t else "")

    # Select and return essential columns only
    columns_to_keep = [
        "id",
        "idMal",
        "title_romaji",
        "title_english",
        "averageScore",
        "trending",
    ]
    return df[columns_to_keep]


def calculate_aggregated_scores(
    df_kaggle: pd.DataFrame, df_jikan: pd.DataFrame, df_anilist: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate aggregated scores from multiple sources.

    Merges scores from Kaggle, Jikan, and AniList into a unified score table.
    AniList scores are normalized from 0-100 to 0-10 scale.

    Args:
        df_kaggle: Kaggle data with 'anime_id' and 'rating'
        df_jikan: Jikan data with 'mal_id' and 'score'
        df_anilist: AniList data with 'idMal' and 'averageScore'

    Returns:
        DataFrame with columns: anime_id, mal_score, anilist_score, avg_score

    Notes:
        - mal_score prioritizes Jikan over Kaggle (more accurate)
        - anilist_score is normalized to 0-10 scale (divided by 10)
        - avg_score is mean of available scores (ignores NaN)
    """
    # Rename mal_id → anime_id in Jikan data
    df_jikan_renamed = df_jikan.rename(columns={"mal_id": "anime_id"})

    # Left join to keep all Kaggle animes
    df_merged = df_kaggle[["anime_id", "rating"]].merge(
        df_jikan_renamed[["anime_id", "score"]], on="anime_id", how="left"
    )

    # Rename idMal → anime_id in AniList data
    df_anilist_renamed = df_anilist.rename(columns={"idMal": "anime_id"})

    # Left join with AniList data
    df_merged = df_merged.merge(
        df_anilist_renamed[["anime_id", "averageScore"]], on="anime_id", how="left"
    )

    # mal_score: Jikan preferred, fallback to Kaggle
    df_merged["mal_score"] = df_merged["score"].fillna(df_merged["rating"])

    # anilist_score: Normalize 0-100 → 0-10 scale
    df_merged["anilist_score"] = df_merged["averageScore"] / 10

    # avg_score: Mean of both scores (ignores NaN)
    df_merged["avg_score"] = df_merged[["mal_score", "anilist_score"]].mean(axis=1)

    # Return only essential columns
    return df_merged[["anime_id", "mal_score", "anilist_score", "avg_score"]]
