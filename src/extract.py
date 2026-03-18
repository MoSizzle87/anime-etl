"""
Data extraction module for anime ETL pipeline.
Handles extraction from Kaggle CSV, Jikan API, and AniList GraphQL.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, cast

import pandas as pd
import requests
from ratelimit import limits, sleep_and_retry
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential


# --- TASK 1: Kaggle CSV ---
def extract_kaggle_csv(file_path: str) -> pd.DataFrame:
    """
    Extract anime data from Kaggle CSV file.

    Args:
        file_path: Path to anime.csv

    Returns:
        Raw pandas DataFrame with anime data

    Raises:
        OSError: If CSV file cannot be read (includes FileNotFoundError)
        ValueError: If CSV is empty or missing required columns
    """
    try:
        df = pd.read_csv(file_path)
    except OSError as e:
        raise OSError(f"Failed to read CSV file at {file_path}: {e}") from e

    # Validation: empty check
    if df.empty:
        raise ValueError(f"CSV file is empty: {file_path}")

    # Validation: columns check
    expected_cols = [
        "anime_id",
        "name",
        "genre",
        "type",
        "episodes",
        "rating",
        "members",
    ]
    missing_cols = set(expected_cols) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing columns in CSV: {missing_cols}")

    return df


# --- TASK 2: Jikan REST API ---
def should_retry_http_error(exception: BaseException) -> bool:
    """
    Determine if HTTP error should be retried.
    Retry on 429 (rate limit) and 5xx (server errors).

    Args:
        exception: Exception raised during request

    Returns:
        True if should retry, False otherwise
    """
    if isinstance(exception, requests.exceptions.HTTPError):
        status_code = exception.response.status_code
        # Retry only if 429 or >= 500
        return status_code == 429 or status_code >= 500

    # If not HTTPError, check for Timeout/ConnectionError
    return isinstance(
        exception, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)
    )


@sleep_and_retry  # type: ignore[misc]
@limits(calls=3, period=1)  # type: ignore[misc]
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception(should_retry_http_error),
)
def fetch_anime_jikan(anime_id: int, base_url: str) -> Optional[Dict[str, Any]]:
    """
    Fetch single anime from Jikan API with rate limiting and retries.

    Args:
        anime_id: MAL anime ID
        base_url: Jikan API base URL

    Returns:
        Anime data dict, or None if anime not found (404)

    Raises:
        requests.HTTPError: On 4xx (except 404) after retries
        requests.RequestException: On network errors after retries
    """
    try:
        response = requests.get(f"{base_url}/anime/{anime_id}", timeout=30)
        response.raise_for_status()
        return cast(Dict[str, Any], response.json()["data"])
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return None
        raise


def extract_jikan_api(anime_ids: List[int], base_url: str) -> List[Dict[str, Any]]:
    """
    Extract anime details from Jikan API for multiple anime IDs.

    Args:
        anime_ids: List of MAL anime IDs to fetch
        base_url: Jikan API base URL (from config)

    Returns:
        List of anime data dicts (excludes 404s)
    """
    results = []
    for anime_id in anime_ids:
        data = fetch_anime_jikan(anime_id, base_url)
        if data is not None:
            results.append(data)
    return results


# --- TASK 3: AniList GraphQL ---
def load_graphql_query(filename: str) -> str:
    """Load GraphQL query from file."""
    query_path = Path(__file__).parent.parent / "queries" / filename

    if not query_path.exists():
        raise FileNotFoundError(f"GraphQL query not found: {query_path}")

    return query_path.read_text(encoding="utf-8")


@sleep_and_retry  # type: ignore[misc]
@limits(calls=90, period=60)  # type: ignore[misc]
def extract_anilist_graphql(
    query: str, variables: Dict[str, Any], api_url: str
) -> Dict[str, Any]:
    """
    Extract anime data from AniList GraphQL API.

    Args:
        query: GraphQL query string
        variables: Query variables (dict)
        api_url: AniList API endpoint

    Returns:
        Response data from AniList API

    Raises:
        requests.HTTPError: On 4xx/5xx errors
        requests.RequestException: On network/API errors
    """
    response = requests.post(
        api_url, json={"query": query, "variables": variables}, timeout=10
    )
    response.raise_for_status()
    return cast(Dict[str, Any], response.json())
