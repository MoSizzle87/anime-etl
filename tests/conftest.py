"""
Pytest configuration and fixtures.
"""

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src.config import load_config


@pytest.fixture
def sample_anime_data():
    """Sample anime data for testing."""
    return pd.DataFrame(
        {
            "anime_id": [1, 2, 3],
            "name": ["Cowboy Bebop", "Naruto", "Death Note"],
            "genre": ["Action, Sci-Fi", "Action, Adventure", "Mystery, Thriller"],
            "type": ["TV", "TV", "TV"],
            "episodes": [26, 220, 37],
            "rating": [8.78, 8.23, 9.0],
            "members": [1000000, 500000, 750000],
        }
    )


@pytest.fixture
def temp_csv_file(tmp_path, sample_anime_data):
    """Create a temporary CSV file for testing."""
    csv_file = tmp_path / "test_anime.csv"
    sample_anime_data.to_csv(csv_file, index=False)
    return csv_file


@pytest.fixture(scope="function")
def test_db_engine():
    """
    Create a test database engine using the existing PostgreSQL container.
    Creates a separate test schema for isolation.
    """
    # Use existing PostgreSQL container
    config = load_config()

    # Create engine
    connection_string = (
        f"postgresql+psycopg2://{config['db_user']}:{config['db_password']}"
        f"@{config['db_host']}:{config['db_port']}/{config['db_name']}"
    )
    engine = create_engine(connection_string)

    # Setup: Clean any existing test tables
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS anime_studio CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS anime_genre CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS f_anime_ratings CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS d_anime CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS d_genre CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS d_studio CASCADE"))
        conn.commit()

    yield engine

    # Teardown: Clean up after tests
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS anime_studio CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS anime_genre CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS f_anime_ratings CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS d_anime CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS d_genre CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS d_studio CASCADE"))
        conn.commit()

    engine.dispose()


@pytest.fixture
def sample_dimension_data():
    """Sample data for dimension tables."""
    df_anime = pd.DataFrame(
        {
            "anime_id": [1, 2, 3],
            "title": ["Cowboy Bebop", "Naruto", "Death Note"],
            "type": ["TV", "TV", "TV"],
            "episodes": [26, 220, 37],
            "synopsis": ["In 2071...", "Before Naruto...", "Light Yagami..."],
        }
    )

    df_genres = pd.DataFrame(
        {"genre_name": ["Action", "Sci-Fi", "Adventure", "Mystery", "Thriller"]}
    )

    df_studios = pd.DataFrame(
        {"studio_name": ["Sunrise", "Studio Pierrot", "Madhouse"]}
    )

    return df_anime, df_genres, df_studios
