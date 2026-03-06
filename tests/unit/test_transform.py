"""
Unit tests for transformation module.
"""

import pandas as pd
import pytest

from src.transform import deduplicate_animes, normalize_title


class TestNormalizeTitle:
    """Tests for normalize_title function."""

    def test_normalize_basic(self):
        """Test basic title normalization."""
        result = normalize_title("Cowboy Bebop")
        assert result == "cowboy bebop"

    def test_normalize_punctuation(self):
        """Test punctuation removal."""
        result = normalize_title("Fullmetal Alchemist: Brotherhood")
        assert result == "fullmetal alchemist brotherhood"

    def test_normalize_accents(self):
        """Test accent removal."""
        result = normalize_title("Café Enchanté")
        assert result == "cafe enchante"


# TODO: Add more tests
