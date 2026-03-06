"""
Unit tests for extraction module.
"""

import pytest

from src.extract import extract_kaggle_csv


class TestExtractKaggleCsv:
    """Tests for extract_kaggle_csv function."""

    def test_extract_valid_csv(self, temp_csv_file):
        """Test extraction from valid CSV file."""
        df = extract_kaggle_csv(str(temp_csv_file))
        assert len(df) == 3
        assert "anime_id" in df.columns
        assert "name" in df.columns

    def test_extract_missing_file(self):
        """Test extraction from non-existent file."""
        with pytest.raises(OSError):
            extract_kaggle_csv("non_existent.csv")


# TODO: Add more tests
