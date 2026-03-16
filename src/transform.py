"""
Data transformation module for anime ETL pipeline.
Handles normalization, fuzzy matching, deduplication, and aggregations.
"""

import re
import unicodedata
from typing import Any, Dict, List

import pandas as pd
from rapidfuzz import fuzz, process


# --- TASK 1: Normalize titles ---
def normalize_title(title: str) -> str:
    """
    Normalize anime title for fuzzy matching.
    Converts to lowercase, removes accents and punctuation.

    Args:
        title: Raw anime title

    Returns:
        Normalized title string
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
    Uses RapidFuzz to compare normalized titles.

    Args:
        df1: First DataFrame (e.g., Kaggle data)
        df2: Second DataFrame (e.g., Jikan data)
        title_col1: Column name for titles in df1
        title_col2: Column name for titles in df2
        threshold: Minimum similarity score (0-100, default: 85)

    Returns:
        DataFrame with matched pairs (index_df1, index_df2, titles, score)
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
    Keeps first occurrence when duplicates are found.

    Args:
        df: DataFrame with anime data
        title_col: Column name containing titles
        threshold: Similarity threshold (0-100, default: 90)

    Returns:
        DataFrame with duplicates removed
    """

    indices_to_drop = set()
    seen_indices = set()

    # Normalize all titles once (performance optimization)
    normalized_titles = df[title_col].apply(normalize_title)

    for idx in df.index:
        # Skip if already marked as duplicate
        if idx in seen_indices:
            continue

        title = normalized_titles.loc[idx]

        # Find all matches for this title using RapidFuzz process
        # This is MUCH faster than manual double loop (implemented in C++)
        matches = process.extract(
            title,
            normalized_titles,
            scorer=fuzz.ratio,
            score_cutoff=threshold,
            limit=None,  # Return all matches above threshold
        )

        # Process matches
        for match_title, score, match_idx in matches:
            if match_idx != idx and match_idx not in seen_indices:
                # Keep the first occurrence (lower index)
                if match_idx > idx:
                    indices_to_drop.add(match_idx)
                    seen_indices.add(match_idx)

    # Return DataFrame without duplicates
    return df.drop(index=list(indices_to_drop)).reset_index(drop=True)


# --- TASK 3: Convert Jikan & AniList data to df ---
def convert_jikan_to_dataframe(jikan_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convert Jikan API response to DataFrame.
    Flattens nested fields (studios, genres) into comma-separated strings.

    Args:
        jikan_data: List of anime dicts from extract_jikan_api()

    Returns:
        DataFrame with essential columns or empty DataFrame if no data
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
    Extracts media from nested structure and flattens title fields.

    Args:
        anilist_data: GraphQL response from extract_anilist_graphql()

    Returns:
        DataFrame with anime data or empty DataFrame if no media found
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
    df_merged["mal_score"] = (
        df_merged["score"].fillna(df_merged["rating"]).infer_objects(copy=False)
    )

    # anilist_score: Normalize 0-100 → 0-10 scale
    df_merged["anilist_score"] = df_merged["averageScore"] / 10

    # avg_score: Mean of both scores (ignores NaN)
    df_merged["avg_score"] = df_merged[["mal_score", "anilist_score"]].mean(axis=1)

    # Return only essential columns
    return df_merged[["anime_id", "mal_score", "anilist_score", "avg_score"]]
