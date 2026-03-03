"""
Main ETL pipeline orchestrator.
Executes the full Extract → Transform → Load workflow.
"""

import pandas as pd
from sqlalchemy import text

from src.config import get_db_engine, load_config
from src.extract import extract_anilist_graphql, extract_jikan_api, extract_kaggle_csv
from src.load import create_schema, drop_schema, load_dimensions, load_facts
from src.transform import (
    calculate_aggregated_scores,
    convert_anilist_to_dataframe,
    convert_jikan_to_dataframe,
    deduplicate_animes,
)


def run_pipeline():
    """
    Execute full ETL pipeline.

    Steps:
    1. EXTRACT: Load data from Kaggle CSV, Jikan API, AniList GraphQL
    2. TRANSFORM: Clean, deduplicate, aggregate scores
    3. LOAD: Create schema and insert into PostgreSQL
    """
    print("=" * 80)
    print("🎌 ANIME ETL PIPELINE - START")
    print("=" * 80)

    # Load configuration
    config = load_config()
    engine = get_db_engine()

    # ========== EXTRACT ==========
    print("\n📥 PHASE 1: EXTRACT")
    print("-" * 80)

    # Extract Kaggle CSV (full dataset)
    print("  → Loading Kaggle dataset...")
    csv_path = f"{config['data_raw_path']}/anime.csv"
    df_kaggle = extract_kaggle_csv(csv_path)
    print(f"    ✓ Loaded {len(df_kaggle)} animes from Kaggle")

    # Extract Jikan API (top 500 most popular animes)
    print("  → Fetching from Jikan API (top 2000 animes)...")
    print("    ⏱️  This will take ~28 minutes (rate limit: 3 req/sec)...")
    top_anime_ids = df_kaggle.nlargest(2000, "members")["anime_id"].tolist()
    jikan_data = extract_jikan_api(
        anime_ids=top_anime_ids, base_url=config["jikan_api_base_url"]
    )
    print(f"    ✓ Fetched {len(jikan_data)} animes from Jikan API")

    # Extract AniList GraphQL (top 50 trending animes)
    print("  → Fetching from AniList GraphQL (top 50 trending)...")
    anilist_query = """
    query ($page: Int, $perPage: Int) {
      Page(page: $page, perPage: $perPage) {
        media(type: ANIME, sort: TRENDING_DESC) {
          id
          idMal
          title {
            romaji
            english
          }
          averageScore
          trending
        }
      }
    }
    """
    anilist_data = extract_anilist_graphql(
        query=anilist_query,
        variables={"page": 1, "perPage": 50},
        api_url=config["anilist_api_url"],
    )
    print(f"    ✓ Fetched AniList trending data")

    # ========== TRANSFORM ==========
    print("\n🔄 PHASE 2: TRANSFORM")
    print("-" * 80)

    # Deduplicate Kaggle data
    print("  → Deduplicating animes...")
    print("    ⏱️  This will take ~10 minutes (fuzzy matching 12k animes)...")
    df_kaggle_clean = deduplicate_animes(df_kaggle, "name", threshold=90)
    print(f"    ✓ Removed {len(df_kaggle) - len(df_kaggle_clean)} duplicates")

    # Convert Jikan data to DataFrame
    df_jikan = None
    if jikan_data:
        print("  → Converting Jikan data...")
        df_jikan = convert_jikan_to_dataframe(jikan_data)
        print(f"    ✓ Converted {len(df_jikan)} Jikan records")

    # Convert AniList data to DataFrame
    df_anilist = None
    if anilist_data:
        print("  → Converting AniList data...")
        df_anilist = convert_anilist_to_dataframe(anilist_data)
        print(f"    ✓ Converted {len(df_anilist)} AniList records")

    # Calculate aggregated scores (Kaggle + Jikan + AniList)
    print("  → Calculating aggregated scores...")
    df_scores = calculate_aggregated_scores(
        df_kaggle=df_kaggle_clean[["anime_id", "rating"]],
        df_jikan=(
            df_jikan
            if df_jikan is not None
            else pd.DataFrame(columns=["mal_id", "score"])
        ),
        df_anilist=(
            df_anilist
            if df_anilist is not None
            else pd.DataFrame(columns=["idMal", "averageScore"])
        ),
    )
    print(f"    ✓ Calculated scores for {len(df_scores)} animes")

    # Merge Jikan data for synopsis and studios
    if df_jikan is not None:
        print("  → Merging Kaggle + Jikan data...")
        df_kaggle_clean = df_kaggle_clean.merge(
            df_jikan[["mal_id", "synopsis", "studios"]],
            left_on="anime_id",
            right_on="mal_id",
            how="left",
            suffixes=("", "_jikan"),
        )
        df_kaggle_clean["synopsis"] = df_kaggle_clean["synopsis"].fillna("")
        enriched_count = (
            df_kaggle_clean["synopsis"].apply(lambda x: len(str(x)) > 0).sum()
        )
        print(f"    ✓ Enriched {enriched_count} animes with Jikan data")
    else:
        df_kaggle_clean["synopsis"] = ""
        df_kaggle_clean["studios"] = ""

    # Prepare dimension: d_anime
    print("  → Preparing anime dimension...")
    df_anime = df_kaggle_clean[["anime_id", "name", "type", "episodes"]].copy()
    df_anime.rename(columns={"name": "title"}, inplace=True)
    df_anime["synopsis"] = df_kaggle_clean.get("synopsis", "")
    # Clean episodes column: convert "Unknown" to None
    df_anime["episodes"] = pd.to_numeric(df_anime["episodes"], errors="coerce")
    print(f"    ✓ Prepared {len(df_anime)} animes")

    # Prepare dimension: d_genre
    print("  → Preparing genre dimension...")
    all_genres = []
    for genres_str in df_kaggle_clean["genre"].dropna():
        genres_list = [g.strip() for g in str(genres_str).split(",")]
        all_genres.extend(genres_list)
    unique_genres = sorted(set(all_genres))
    df_genres = pd.DataFrame({"genre_name": unique_genres})
    print(f"    ✓ Found {len(df_genres)} unique genres")

    # Prepare dimension: d_studio (from Jikan data)
    print("  → Preparing studio dimension...")
    all_studios = []
    if "studios" in df_kaggle_clean.columns:
        for studios_str in df_kaggle_clean["studios"].dropna():
            if studios_str and str(studios_str).strip() != "":
                studios_list = [s.strip() for s in str(studios_str).split(",")]
                all_studios.extend(studios_list)

    unique_studios = sorted(set(all_studios)) if all_studios else ["Unknown"]
    df_studios = pd.DataFrame({"studio_name": unique_studios})
    print(f"    ✓ Prepared {len(df_studios)} studios")

    # Prepare fact: f_anime_ratings (from aggregated scores)
    print("  → Preparing ratings fact table...")
    df_ratings = df_scores[
        ["anime_id", "mal_score", "anilist_score", "avg_score"]
    ].copy()
    print(f"    ✓ Prepared {len(df_ratings)} ratings")

    # Prepare linking table: anime_genre
    print("  → Preparing anime-genre relationships...")
    anime_genre_list = []
    for _, row in df_kaggle_clean.iterrows():
        anime_id = row["anime_id"]
        genres_str = row["genre"]
        if pd.notna(genres_str):
            genres_list = [g.strip() for g in str(genres_str).split(",")]
            for genre in genres_list:
                anime_genre_list.append({"anime_id": anime_id, "genre_name": genre})
    df_anime_genres = pd.DataFrame(anime_genre_list)

    # Remove duplicates (same anime_id + genre_name combination)
    df_anime_genres = df_anime_genres.drop_duplicates(subset=["anime_id", "genre_name"])

    print(f"    ✓ Prepared {len(df_anime_genres)} anime-genre relationships")

    # Prepare linking table: anime_studio
    print("  → Preparing anime-studio relationships...")
    anime_studio_list = []
    if "studios" in df_kaggle_clean.columns:
        for _, row in df_kaggle_clean.iterrows():
            anime_id = row["anime_id"]
            studios_str = row.get("studios")
            if pd.notna(studios_str) and str(studios_str).strip() != "":
                studios_list = [s.strip() for s in str(studios_str).split(",")]
                for studio in studios_list:
                    anime_studio_list.append(
                        {"anime_id": anime_id, "studio_name": studio}
                    )

    df_anime_studios = (
        pd.DataFrame(anime_studio_list)
        if anime_studio_list
        else pd.DataFrame(columns=["anime_id", "studio_name"])
    )
    # Remove duplicates (same anime_id + studio_name combination)
    if len(df_anime_studios) > 0:
        df_anime_studios = df_anime_studios.drop_duplicates(
            subset=["anime_id", "studio_name"]
        )

    print(f"    ✓ Prepared {len(df_anime_studios)} anime-studio relationships")

    # ========== LOAD ==========
    print("\n💾 PHASE 3: LOAD")
    print("-" * 80)

    # Drop existing tables (for development - ensures clean state)
    print("  → Dropping existing tables...")
    drop_schema(engine)

    # Create schema
    print("  → Creating database schema...")
    create_schema(engine)

    # Load dimensions
    print("  → Loading dimensions...")
    load_dimensions(engine, df_anime, df_genres, df_studios)

    # Load facts
    print("  → Loading facts and linking tables...")
    load_facts(engine, df_ratings, df_anime_genres, df_anime_studios)


if __name__ == "__main__":
    run_pipeline()
