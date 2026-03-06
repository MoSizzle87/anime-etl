"""
Pytest configuration and fixtures.
"""

from pathlib import Path

import pandas as pd
import pytest


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
