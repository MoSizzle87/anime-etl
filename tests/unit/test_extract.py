"""
Unit tests for extraction module.
"""

from pathlib import Path

import pandas as pd
import pytest

from src.extract import extract_kaggle_csv


class TestExtractKaggleCsv:
    """Tests for extract_kaggle_csv function."""

    def test_extract_valid_csv(self, tmp_path):
        """Should extract data from valid CSV file."""
        # ARRANGE - Create a temporary CSV file
        csv_file = tmp_path / "test_anime.csv"
        test_data = pd.DataFrame(
            {
                "anime_id": [1, 2, 3],
                "name": ["Cowboy Bebop", "Naruto", "Death Note"],
                "genre": ["Action, Sci-Fi", "Action", "Mystery"],
                "type": ["TV", "TV", "TV"],
                "episodes": [26, 220, 37],
                "rating": [8.78, 8.23, 9.0],
                "members": [1000000, 500000, 750000],
            }
        )
        test_data.to_csv(csv_file, index=False)

        # ACT - Extract the CSV
        result = extract_kaggle_csv(str(csv_file))

        # ASSERT - Verify the result
        assert len(result) == 3
        assert "anime_id" in result.columns
        assert "name" in result.columns
        assert result.iloc[0]["name"] == "Cowboy Bebop"

    def test_extract_missing_file(self):
        """Should raise OSError for missing file."""
        # extract.py wraps FileNotFoundError into OSError
        with pytest.raises(OSError, match="Failed to read CSV"):
            extract_kaggle_csv("non_existent.csv")

    def test_extract_empty_csv(self, tmp_path):
        """Should raise ValueError for empty CSV file."""
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("anime_id,name,genre\n")  # Header only

        # extract.py raises ValueError for empty DataFrames
        with pytest.raises(ValueError, match="CSV file is empty"):
            extract_kaggle_csv(str(csv_file))

    def test_extract_missing_columns(self, tmp_path):
        """Should raise ValueError if required columns are missing."""
        csv_file = tmp_path / "invalid.csv"
        # Missing 'name' column
        test_data = pd.DataFrame({"anime_id": [1, 2], "genre": ["Action", "Drama"]})
        test_data.to_csv(csv_file, index=False)

        with pytest.raises(ValueError, match="Missing columns in CSV"):
            extract_kaggle_csv(str(csv_file))
