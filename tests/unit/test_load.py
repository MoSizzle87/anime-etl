"""
Unit tests for loading module.
"""

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src.load import create_schema, drop_schema, load_dimensions, load_facts


@pytest.fixture
def test_engine():
    """Create test database engine."""
    # Use in-memory SQLite for testing
    engine = create_engine("sqlite:///:memory:")
    yield engine
    engine.dispose()


class TestSchema:
    """Tests for schema creation and deletion."""

    def test_create_schema(self, test_db_engine):
        """Should create all tables in star schema."""
        # ACT
        create_schema(test_db_engine)

        # ASSERT - Verify tables exist
        with test_db_engine.connect() as conn:
            # Check d_anime
            result = conn.execute(
                text(
                    "SELECT EXISTS (SELECT FROM information_schema.tables "
                    "WHERE table_name = 'd_anime')"
                )
            )
            assert result.scalar() is True

            # Check d_genre
            result = conn.execute(
                text(
                    "SELECT EXISTS (SELECT FROM information_schema.tables "
                    "WHERE table_name = 'd_genre')"
                )
            )
            assert result.scalar() is True

            # Check d_studio
            result = conn.execute(
                text(
                    "SELECT EXISTS (SELECT FROM information_schema.tables "
                    "WHERE table_name = 'd_studio')"
                )
            )
            assert result.scalar() is True

            # Check f_anime_ratings
            result = conn.execute(
                text(
                    "SELECT EXISTS (SELECT FROM information_schema.tables "
                    "WHERE table_name = 'f_anime_ratings')"
                )
            )
            assert result.scalar() is True

    def test_drop_schema(self, test_db_engine):
        """Should drop all tables."""
        # ARRANGE - Create tables first
        create_schema(test_db_engine)

        # ACT
        drop_schema(test_db_engine)

        # ASSERT - Verify tables don't exist
        with test_db_engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_name IN ('d_anime', 'd_genre', 'd_studio', "
                    "'f_anime_ratings', 'anime_genre', 'anime_studio')"
                )
            )
            assert result.scalar() == 0


class TestLoadDimensions:
    """Tests for load_dimensions function."""

    def test_load_dimensions_successful(self, test_db_engine, sample_dimension_data):
        """Should load dimension tables with data."""
        # ARRANGE
        create_schema(test_db_engine)
        df_anime, df_genres, df_studios = sample_dimension_data

        # ACT
        load_dimensions(test_db_engine, df_anime, df_genres, df_studios)

        # ASSERT
        with test_db_engine.connect() as conn:
            # Check d_anime
            result = conn.execute(text("SELECT COUNT(*) FROM d_anime"))
            assert result.scalar() == 3

            # Check d_genre
            result = conn.execute(text("SELECT COUNT(*) FROM d_genre"))
            assert result.scalar() == 5

            # Check d_studio
            result = conn.execute(text("SELECT COUNT(*) FROM d_studio"))
            assert result.scalar() == 3

            # Verify anime data
            result = conn.execute(text("SELECT title FROM d_anime WHERE anime_id = 1"))
            assert result.scalar() == "Cowboy Bebop"

    def test_load_dimensions_empty_dataframes(self, test_db_engine):
        """Should handle empty DataFrames."""
        create_schema(test_db_engine)

        df_anime = pd.DataFrame(
            columns=["anime_id", "title", "type", "episodes", "synopsis"]
        )
        df_genres = pd.DataFrame(columns=["genre_name"])
        df_studios = pd.DataFrame(columns=["studio_name"])

        load_dimensions(test_db_engine, df_anime, df_genres, df_studios)

        with test_db_engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM d_anime"))
            assert result.scalar() == 0


class TestLoadFacts:
    """Tests for load_facts function."""

    def test_load_facts_successful(self, test_db_engine, sample_dimension_data):
        """Should load fact and linking tables."""
        # ARRANGE
        create_schema(test_db_engine)
        df_anime, df_genres, df_studios = sample_dimension_data
        load_dimensions(test_db_engine, df_anime, df_genres, df_studios)

        df_ratings = pd.DataFrame(
            {
                "anime_id": [1, 2, 3],
                "mal_score": [8.78, 8.23, 9.0],
                "anilist_score": [8.6, 8.2, 9.1],
                "avg_score": [8.69, 8.22, 9.05],
            }
        )

        df_anime_genres = pd.DataFrame(
            {
                "anime_id": [1, 1, 2, 2, 3],
                "genre_name": ["Action", "Sci-Fi", "Action", "Adventure", "Mystery"],
            }
        )

        df_anime_studios = pd.DataFrame(
            {
                "anime_id": [1, 2, 3],
                "studio_name": ["Sunrise", "Studio Pierrot", "Madhouse"],
            }
        )

        # ACT
        load_facts(test_db_engine, df_ratings, df_anime_genres, df_anime_studios)

        # ASSERT
        with test_db_engine.connect() as conn:
            # Check f_anime_ratings
            result = conn.execute(text("SELECT COUNT(*) FROM f_anime_ratings"))
            assert result.scalar() == 3

            # Check anime_genre
            result = conn.execute(text("SELECT COUNT(*) FROM anime_genre"))
            assert result.scalar() == 5

            # Check anime_studio
            result = conn.execute(text("SELECT COUNT(*) FROM anime_studio"))
            assert result.scalar() == 3

            # Verify rating data
            result = conn.execute(
                text("SELECT mal_score FROM f_anime_ratings WHERE anime_id = 1")
            )
            assert result.scalar() == 8.78
