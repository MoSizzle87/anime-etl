"""
Integration tests for the full pipeline.
"""

import pandas as pd
import pytest
from sqlalchemy import text

from src.config import get_db_engine, load_config
from src.extract import extract_kaggle_csv
from src.load import create_schema, drop_schema, load_dimensions
from src.transform import deduplicate_animes


class TestPipeline:
    """End-to-end pipeline tests."""

    def test_full_pipeline_small_dataset(self, tmp_path):
        """Test pipeline with small dataset (10 animes)."""
        # ARRANGE - Create small test CSV
        test_data = pd.DataFrame(
            {
                "anime_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                "name": [
                    "Cowboy Bebop",
                    "Naruto",
                    "Death Note",
                    "One Piece",
                    "Attack on Titan",
                    "Fullmetal Alchemist",
                    "Steins;Gate",
                    "Hunter x Hunter",
                    "Code Geass",
                    "Demon Slayer",
                ],
                "genre": [
                    "Action, Sci-Fi",
                    "Action, Adventure",
                    "Mystery, Thriller",
                    "Action, Adventure",
                    "Action, Drama",
                    "Action, Adventure",
                    "Sci-Fi, Thriller",
                    "Action, Adventure",
                    "Action, Mecha",
                    "Action, Supernatural",
                ],
                "type": ["TV"] * 10,
                "episodes": [26, 220, 37, 1000, 87, 64, 24, 148, 50, 26],
                "rating": [8.78, 8.23, 9.0, 8.7, 9.1, 9.1, 9.1, 9.0, 8.7, 8.6],
                "members": [1000000] * 10,
            }
        )

        csv_file = tmp_path / "small_anime.csv"
        test_data.to_csv(csv_file, index=False)

        # ACT - Run mini pipeline
        df = extract_kaggle_csv(str(csv_file))
        df_clean = deduplicate_animes(df, "name", threshold=90)

        # Prepare dimensions
        df_anime = df_clean[["anime_id", "name", "type", "episodes"]].copy()
        df_anime.rename(columns={"name": "title"}, inplace=True)
        df_anime["synopsis"] = ""
        df_anime["episodes"] = pd.to_numeric(df_anime["episodes"], errors="coerce")

        # Extract genres
        all_genres = []
        for genres_str in df_clean["genre"].dropna():
            all_genres.extend([g.strip() for g in str(genres_str).split(",")])
        df_genres = pd.DataFrame({"genre_name": sorted(set(all_genres))})

        df_studios = pd.DataFrame({"studio_name": ["Test Studio"]})

        # Load to test DB
        engine = get_db_engine()
        drop_schema(engine)
        create_schema(engine)
        load_dimensions(engine, df_anime, df_genres, df_studios)

        # ASSERT
        with engine.begin() as conn:
            # Check anime count
            result = conn.execute(text("SELECT COUNT(*) FROM d_anime"))
            assert result.scalar() == 10

            # Check genres
            result = conn.execute(text("SELECT COUNT(*) FROM d_genre"))
            genre_count = result.scalar()
            assert (
                genre_count >= 5
            )  # At least Action, Sci-Fi, Adventure, Mystery, Thriller

            # Check specific anime exists
            result = conn.execute(text("SELECT title FROM d_anime WHERE anime_id = 1"))
            assert result.scalar() == "Cowboy Bebop"

        # Cleanup
        drop_schema(engine)
        engine.dispose()
