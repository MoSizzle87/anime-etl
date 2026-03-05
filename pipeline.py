"""
Main ETL pipeline orchestrator.
Executes the full Extract → Transform → Load workflow.
"""

import time

import pandas as pd
from sqlalchemy import text

from src.config import get_db_engine, load_config
from src.extract import extract_anilist_graphql, extract_jikan_api, extract_kaggle_csv
from src.load import create_schema, drop_schema, load_dimensions, load_facts
from src.logger import format_duration, setup_logging
from src.transform import (
    calculate_aggregated_scores,
    convert_anilist_to_dataframe,
    convert_jikan_to_dataframe,
    deduplicate_animes,
)

# Setup logging
logger = setup_logging()


def run_pipeline():
    """
    Execute full ETL pipeline.

    Steps:
        1. EXTRACT: Load data from Kaggle CSV, Jikan API, AniList GraphQL
        2. TRANSFORM: Clean, deduplicate, aggregate scores
        3. LOAD: Create schema and insert into PostgreSQL
    """
    pipeline_start = time.time()

    logger.info("=" * 80)
    logger.info("🎌 ANIME ETL PIPELINE - START")
    logger.info("=" * 80)

    # Load configuration
    config = load_config()
    engine = get_db_engine()

    # ========== EXTRACT ==========
    logger.info("\n📥 PHASE 1: EXTRACT")
    logger.info("-" * 80)

    # Extract Kaggle CSV (full dataset)
    logger.info("  → Loading Kaggle dataset...")
    start = time.time()
    csv_path = f"{config['data_raw_path']}/anime.csv"
    df_kaggle = extract_kaggle_csv(csv_path)
    elapsed = time.time() - start
    logger.info(
        f"    ✓ Loaded {len(df_kaggle)} animes (completed in {format_duration(elapsed)})"
    )
    # Extract Jikan API (top 500 most popular animes)
    logger.info("  → Fetching from Jikan API (top 2000 animes)...")
    start = time.time()
    top_anime_ids = df_kaggle.nlargest(2000, "members")["anime_id"].tolist()
    jikan_data = extract_jikan_api(
        anime_ids=top_anime_ids, base_url=config["jikan_api_base_url"]
    )
    elapsed = time.time() - start
    logger.info(
        f"    ✓ Fetched {len(jikan_data)} animes (completed in {format_duration(elapsed)})"
    )

    # Extract AniList GraphQL (top 50 trending animes)
    logger.info("  → Fetching from AniList GraphQL (top 50 trending)...")
    start = time.time()
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
    elapsed = time.time() - start
    logger.info(
        f"    ✓ Fetched AniList trending data (completed in {format_duration(elapsed)})"
    )

    # ========== TRANSFORM ==========
    logger.info("\n🔄 PHASE 2: TRANSFORM")
    logger.info("-" * 80)

    # Deduplicate Kaggle data
    logger.info("  → Deduplicating animes...")
    start = time.time()
    df_kaggle_clean = deduplicate_animes(df_kaggle, "name", threshold=90)
    elapsed = time.time() - start
    logger.info(
        f"    ✓ Removed {len(df_kaggle) - len(df_kaggle_clean)} duplicates (completed in {format_duration(elapsed)})"
    )

    # Convert Jikan data to DataFrame
    if jikan_data:
        logger.info("  → Converting Jikan data...")
        start = time.time()
        df_jikan = convert_jikan_to_dataframe(jikan_data)
        elapsed = time.time() - start
        logger.info(
            f"    ✓ Converted {len(df_jikan)} Jikan records (completed in {format_duration(elapsed)})"
        )
    else:
        df_jikan = None

    # Convert AniList data to DataFrame
    if anilist_data:
        logger.info("  → Converting AniList data...")
        start = time.time()
        df_anilist = convert_anilist_to_dataframe(anilist_data)
        elapsed = time.time() - start
        logger.info(
            f"    ✓ Converted {len(df_anilist)} AniList records (completed in {format_duration(elapsed)})"
        )
    else:
        df_anilist = None

    # Calculate aggregated scores
    logger.info("  → Calculating aggregated scores...")
    start = time.time()
    df_scores = calculate_aggregated_scores(
        df_kaggle=df_kaggle_clean[["anime_id", "rating"]],
        df_jikan=df_jikan
        if df_jikan is not None
        else pd.DataFrame(columns=["mal_id", "score"]),
        df_anilist=df_anilist
        if df_anilist is not None
        else pd.DataFrame(columns=["idMal", "averageScore"]),
    )
    elapsed = time.time() - start
    logger.info(
        f"    ✓ Calculated scores for {len(df_scores)} animes (completed in {format_duration(elapsed)})"
    )

    # Merge Jikan data for synopsis and studios
    if df_jikan is not None:
        logger.info("  → Merging Kaggle + Jikan data...")
        start = time.time()
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
        elapsed = time.time() - start
        logger.info(
            f"    ✓ Enriched {enriched_count} animes with Jikan data (completed in {format_duration(elapsed)})"
        )
    else:
        df_kaggle_clean["synopsis"] = ""
        df_kaggle_clean["studios"] = ""

    # Prepare dimension: d_anime
    logger.info("  → Preparing anime dimension...")
    start = time.time()
    df_anime = df_kaggle_clean[["anime_id", "name", "type", "episodes"]].copy()
    df_anime.rename(columns={"name": "title"}, inplace=True)
    df_anime["synopsis"] = df_kaggle_clean.get("synopsis", "")
    df_anime["episodes"] = pd.to_numeric(df_anime["episodes"], errors="coerce")
    elapsed = time.time() - start
    logger.info(
        f"    ✓ Prepared {len(df_anime)} animes (completed in {format_duration(elapsed)})"
    )

    # Prepare dimension: d_genre
    logger.info("  → Preparing genre dimension...")
    start = time.time()
    all_genres = []
    for genres_str in df_kaggle_clean["genre"].dropna():
        genres_list = [g.strip() for g in str(genres_str).split(",")]
        all_genres.extend(genres_list)
    unique_genres = sorted(set(all_genres))
    df_genres = pd.DataFrame({"genre_name": unique_genres})
    elapsed = time.time() - start
    logger.info(
        f"    ✓ Found {len(df_genres)} unique genres (completed in {format_duration(elapsed)})"
    )

    # Prepare dimension: d_studio
    logger.info("  → Preparing studio dimension...")
    start = time.time()
    all_studios = []
    if "studios" in df_kaggle_clean.columns:
        for studios_str in df_kaggle_clean["studios"].dropna():
            if studios_str and str(studios_str).strip() != "":
                studios_list = [s.strip() for s in str(studios_str).split(",")]
                all_studios.extend(studios_list)

    unique_studios = sorted(set(all_studios)) if all_studios else ["Unknown"]
    df_studios = pd.DataFrame({"studio_name": unique_studios})
    elapsed = time.time() - start
    logger.info(
        f"    ✓ Prepared {len(df_studios)} studios (completed in {format_duration(elapsed)})"
    )

    # Prepare fact: f_anime_ratings
    logger.info("  → Preparing ratings fact table...")
    start = time.time()
    df_ratings = df_scores[
        ["anime_id", "mal_score", "anilist_score", "avg_score"]
    ].copy()
    elapsed = time.time() - start
    logger.info(
        f"    ✓ Prepared {len(df_ratings)} ratings (completed in {format_duration(elapsed)})"
    )

    # Prepare linking table: anime_genre
    logger.info("  → Preparing anime-genre relationships...")
    start = time.time()
    anime_genre_list = []
    for _, row in df_kaggle_clean.iterrows():
        anime_id = row["anime_id"]
        genres_str = row["genre"]
        if pd.notna(genres_str):
            genres_list = [g.strip() for g in str(genres_str).split(",")]
            for genre in genres_list:
                anime_genre_list.append({"anime_id": anime_id, "genre_name": genre})

    df_anime_genres = pd.DataFrame(anime_genre_list)
    df_anime_genres = df_anime_genres.drop_duplicates(subset=["anime_id", "genre_name"])
    elapsed = time.time() - start
    logger.info(
        f"    ✓ Prepared {len(df_anime_genres)} anime-genre relationships (completed in {format_duration(elapsed)})"
    )

    # Prepare linking table: anime_studio
    logger.info("  → Preparing anime-studio relationships...")
    start = time.time()
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

    if len(df_anime_studios) > 0:
        df_anime_studios = df_anime_studios.drop_duplicates(
            subset=["anime_id", "studio_name"]
        )

    elapsed = time.time() - start
    logger.info(
        f"    ✓ Prepared {len(df_anime_studios)} anime-studio relationships (completed in {format_duration(elapsed)})"
    )

    # ========== LOAD ==========
    logger.info("\n💾 PHASE 3: LOAD")
    logger.info("-" * 80)

    # Drop existing tables
    logger.info("  → Dropping existing tables...")
    start = time.time()
    drop_schema(engine)
    elapsed = time.time() - start
    logger.info(
        f"    ✓ Dropped existing tables (completed in {format_duration(elapsed)})"
    )

    # Create schema
    logger.info("  → Creating database schema...")
    start = time.time()
    create_schema(engine)
    elapsed = time.time() - start
    logger.info(f"    ✓ Schema created (completed in {format_duration(elapsed)})")

    # Load dimensions
    logger.info("  → Loading dimensions...")
    start = time.time()
    load_dimensions(engine, df_anime, df_genres, df_studios)
    elapsed = time.time() - start
    logger.info(f"    ✓ Loaded dimensions (completed in {format_duration(elapsed)})")

    # Load facts
    logger.info("  → Loading facts and linking tables...")
    start = time.time()
    load_facts(engine, df_ratings, df_anime_genres, df_anime_studios)
    elapsed = time.time() - start
    logger.info(
        f"    ✓ Loaded facts and linking tables (completed in {format_duration(elapsed)})"
    )

    # Final summary
    pipeline_elapsed = time.time() - pipeline_start

    logger.info("\n" + "=" * 80)
    logger.info("✅ PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 80)
    logger.info(f"\n⏱️  Total execution time: {format_duration(pipeline_elapsed)}")
    logger.info(f"\n📊 Summary:")
    logger.info(
        f"   - Animes processed: {len(df_kaggle)} → {len(df_anime)} ({len(df_kaggle) - len(df_anime)} duplicates removed)"
    )
    logger.info(
        f"   - Jikan enrichments: {len(jikan_data) if jikan_data else 0} animes"
    )
    logger.info(
        f"   - AniList data: {len(df_anilist) if df_anilist is not None else 0} trending animes"
    )
    logger.info(f"   - Genres: {len(df_genres)}")
    logger.info(f"   - Studios: {len(df_studios)}")
    logger.info(f"   - Anime-Genre links: {len(df_anime_genres)}")
    logger.info(f"   - Anime-Studio links: {len(df_anime_studios)}")


if __name__ == "__main__":
    run_pipeline()
