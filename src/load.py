"""
Data loading module for anime ETL pipeline.
Handles creation of star schema and loading data into PostgreSQL.
"""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def drop_schema(engine: Engine) -> None:
    """
    Drop all tables in the star schema.

    Useful for development to ensure a clean state before recreating tables.

    Args:
        engine: SQLAlchemy engine connected to PostgreSQL
    """
    drop_statements = [
        "DROP TABLE IF EXISTS anime_genre CASCADE",
        "DROP TABLE IF EXISTS anime_studio CASCADE",
        "DROP TABLE IF EXISTS f_anime_ratings CASCADE",
        "DROP TABLE IF EXISTS d_anime CASCADE",
        "DROP TABLE IF EXISTS d_genre CASCADE",
        "DROP TABLE IF EXISTS d_studio CASCADE",
    ]

    with engine.begin() as connection:
        for sql in drop_statements:
            connection.execute(text(sql))


# --- Create Tables ---
def create_schema(engine: Engine) -> None:
    """
    Create star schema tables in PostgreSQL.
    Creates dimensions (anime, genre, studio), fact table (ratings), and linking tables.

    Args:
        engine: SQLAlchemy engine connected to PostgreSQL
    """
    # Define SQL statements for all tables
    sql_statements = [
        # 1. Dimension: Anime metadata
        """
        CREATE TABLE IF NOT EXISTS d_anime (
            anime_id INTEGER PRIMARY KEY,
            title VARCHAR(500) NOT NULL,
            type VARCHAR(50),
            episodes INTEGER,
            synopsis TEXT
        );
        """,
        # 2. Dimension: Genres
        """
        CREATE TABLE IF NOT EXISTS d_genre (
            genre_id SERIAL PRIMARY KEY,
            genre_name VARCHAR(100) UNIQUE NOT NULL
        );
        """,
        # 3. Dimension: Studios
        """
        CREATE TABLE IF NOT EXISTS d_studio (
            studio_id SERIAL PRIMARY KEY,
            studio_name VARCHAR(200) UNIQUE NOT NULL
        );
        """,
        # 4. Fact table: Anime ratings
        """
        CREATE TABLE IF NOT EXISTS f_anime_ratings (
            anime_id INTEGER PRIMARY KEY REFERENCES d_anime(anime_id),
            mal_score FLOAT,
            anilist_score FLOAT,
            avg_score FLOAT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        # 5. Linking table: Anime ↔ Genres (many-to-many)
        """
        CREATE TABLE IF NOT EXISTS anime_genre (
            anime_id INTEGER REFERENCES d_anime(anime_id) ON DELETE CASCADE,
            genre_id INTEGER REFERENCES d_genre(genre_id) ON DELETE CASCADE,
            PRIMARY KEY (anime_id, genre_id)
        );
        """,
        # 6. Linking table: Anime ↔ Studios (many-to-many)
        """
        CREATE TABLE IF NOT EXISTS anime_studio (
            anime_id INTEGER REFERENCES d_anime(anime_id) ON DELETE CASCADE,
            studio_id INTEGER REFERENCES d_studio(studio_id) ON DELETE CASCADE,
            PRIMARY KEY (anime_id, studio_id)
        );
        """,
    ]

    # Execute all CREATE TABLE statements
    with engine.begin() as connection:
        for sql in sql_statements:
            connection.execute(text(sql))


# --- Load dimensions ---
def load_dimensions(
    engine: Engine,
    df_anime: pd.DataFrame,
    df_genres: pd.DataFrame,
    df_studios: pd.DataFrame,
) -> None:
    """
    Load dimension tables (d_anime, d_genre, d_studio).

    Args:
        engine: SQLAlchemy engine
        df_anime: DataFrame with anime metadata
        df_genres: DataFrame with unique genre names
        df_studios: DataFrame with unique studio names
    """

    # Load d_anime
    df_anime[["anime_id", "title", "type", "episodes", "synopsis"]].to_sql(
        "d_anime", engine, if_exists="append", index=False
    )

    # Load d_genre
    df_genres[["genre_name"]].to_sql("d_genre", engine, if_exists="append", index=False)

    # Load d_studio
    df_studios[["studio_name"]].to_sql(
        "d_studio", engine, if_exists="append", index=False
    )


# --- Load facts & kinking tables ---
def load_facts(
    engine: Engine,
    df_ratings: pd.DataFrame,
    df_anime_genres: pd.DataFrame,
    df_anime_studios: pd.DataFrame,
) -> None:
    """
    Load fact table and linking tables.

    Args:
        engine: SQLAlchemy engine
        df_ratings: DataFrame with anime scores
        df_anime_genres: DataFrame with anime-genre relationships
        df_anime_studios: DataFrame with anime-studio relationships
    """
    # 1. Load f_anime_ratings
    df_ratings[["anime_id", "mal_score", "anilist_score", "avg_score"]].to_sql(
        "f_anime_ratings", engine, if_exists="append", index=False
    )

    # 2. Load anime_genre (requires genre_id lookup)
    # Read d_genre to get genre_id mapping
    df_genre_mapping = pd.read_sql("SELECT genre_id, genre_name FROM d_genre", engine)

    # Merge to get genre_id
    df_anime_genres_with_id = df_anime_genres.merge(
        df_genre_mapping, on="genre_name", how="left"
    )

    # Drop rows where genre_id is NULL (genre not found in d_genre)
    df_anime_genres_with_id = df_anime_genres_with_id.dropna(subset=["genre_id"])

    # Convert genre_id to int
    df_anime_genres_with_id["genre_id"] = df_anime_genres_with_id["genre_id"].astype(
        int
    )

    # Insert into anime_genre
    df_anime_genres_with_id[["anime_id", "genre_id"]].to_sql(
        "anime_genre", engine, if_exists="append", index=False
    )

    # 3. Load anime_studio (requires studio_id lookup)
    # Read d_studio to get studio_id mapping
    df_studio_mapping = pd.read_sql(
        "SELECT studio_id, studio_name FROM d_studio", engine
    )

    # Merge to get studio_id
    df_anime_studios_with_id = df_anime_studios.merge(
        df_studio_mapping, on="studio_name", how="left"
    )

    # Drop rows where studio_id is NULL
    df_anime_studios_with_id = df_anime_studios_with_id.dropna(subset=["studio_id"])

    # Convert studio_id to int
    df_anime_studios_with_id["studio_id"] = df_anime_studios_with_id[
        "studio_id"
    ].astype(int)

    # Insert into anime_studio
    df_anime_studios_with_id[["anime_id", "studio_id"]].to_sql(
        "anime_studio", engine, if_exists="append", index=False
    )
