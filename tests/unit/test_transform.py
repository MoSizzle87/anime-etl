"""
Unit tests for transformation module.
"""

import pandas as pd
import pytest

from src.transform import (
    calculate_aggregated_scores,
    convert_anilist_to_dataframe,
    convert_jikan_to_dataframe,
    deduplicate_animes,
    fuzzy_match_titles,
    normalize_title,
)

# ========== FIXTURES ==========


@pytest.fixture
def df_with_duplicates():
    """DataFrame with duplicate anime titles."""
    return pd.DataFrame(
        {
            "anime_id": [1, 2, 3, 4],
            "name": [
                "Cowboy Bebop",
                "Cowboy Bepop",  # Typo (duplicate)
                "Naruto",
                "cowboy bebop",  # Case difference (duplicate)
            ],
        }
    )


# ========== TESTS NORMALIZE ==========


class TestNormalizeTitle:
    """Tests for normalize_title function."""

    def test_lowercase(self):
        """Should convert to lowercase."""
        title = "COWBOY BEBOP"
        result = normalize_title(title)
        assert result == "cowboy bebop"

    def test_remove_punctuation(self):
        """Should remove punctuation."""
        result = normalize_title("Fullmetal Alchemist: Brotherhood")
        assert result == "fullmetal alchemist brotherhood"

    def test_remove_accents(self):
        """Should remove accents."""
        result = normalize_title("Café Enchanté")
        assert result == "cafe enchante"

    def test_remove_extra_spaces(self):
        """Should remove multiple spaces."""
        result = normalize_title("Cowboy    Bebop")
        assert result == "cowboy bebop"

    def test_empty_string(self):
        """Should handle empty string."""
        result = normalize_title("")
        assert result == ""


# ========== FIXTURES DEDUPLICATE ==========


@pytest.fixture
def df_exact_duplicates():
    """DataFrame with exact duplicate titles."""
    return pd.DataFrame(
        {
            "anime_id": [1, 2, 3, 4],
            "name": [
                "Cowboy Bebop",
                "Cowboy Bebop",  # Exact duplicate
                "Naruto",
                "cowboy bebop",  # Case difference
            ],
        }
    )


@pytest.fixture
def df_fuzzy_duplicates():
    """DataFrame with fuzzy duplicate titles (typos)."""
    return pd.DataFrame(
        {
            "anime_id": [1, 2, 3],
            "name": [
                "Cowboy Bebop",
                "Cowboy Bepop",  # Typo (92% similarity)
                "Naruto",
            ],
        }
    )


# ========== TESTS DEDUPLICATE ==========


class TestDeduplicateAnimes:
    """Tests for deduplicate_animes function."""

    def test_removes_exact_duplicates(self, df_exact_duplicates):
        """Should remove exact duplicates with strict threshold."""
        result = deduplicate_animes(df_exact_duplicates, "name", threshold=95)

        # Keeps first occurrence only
        assert len(result) == 2
        assert "Cowboy Bebop" in result["name"].values
        assert "Naruto" in result["name"].values

    def test_removes_fuzzy_duplicates(self, df_fuzzy_duplicates):
        """Should remove fuzzy duplicates (typos) with relaxed threshold."""
        result = deduplicate_animes(df_fuzzy_duplicates, "name", threshold=90)

        # Should detect "Bepop" typo as duplicate
        assert len(result) == 2
        assert "Naruto" in result["name"].values

    def test_keeps_all_if_no_duplicates(self):
        """Should keep all when no duplicates exist."""
        df = pd.DataFrame(
            {"anime_id": [1, 2, 3], "name": ["Cowboy Bebop", "Naruto", "Death Note"]}
        )

        result = deduplicate_animes(df, "name", threshold=90)
        assert len(result) == 3

    def test_empty_dataframe(self):
        """Should handle empty DataFrame."""
        df = pd.DataFrame(columns=["anime_id", "name"])
        result = deduplicate_animes(df, "name")
        assert len(result) == 0


# ========== TESTS CONVERT JIKAN ==========


class TestConvertJikanToDataframe:
    """Tests for convert_jikan_to_dataframe function."""

    def test_convert_valid_jikan_data(self):
        """Should convert Jikan API response to DataFrame."""
        # ARRANGE - Mock Jikan API response
        jikan_data = [
            {
                "mal_id": 1,
                "title": "Cowboy Bebop",
                "synopsis": "In the year 2071...",
                "score": 8.78,
                "scored_by": 500000,
                "studios": [{"name": "Sunrise"}],
                "genres": [{"name": "Action"}, {"name": "Sci-Fi"}],
            },
            {
                "mal_id": 2,
                "title": "Naruto",
                "synopsis": "Moments prior...",
                "score": 8.23,
                "scored_by": 300000,
                "studios": [{"name": "Studio Pierrot"}],
                "genres": [{"name": "Action"}, {"name": "Adventure"}],
            },
        ]

        # ACT
        result = convert_jikan_to_dataframe(jikan_data)

        # ASSERT
        assert len(result) == 2
        assert "mal_id" in result.columns
        assert "title" in result.columns
        assert "synopsis" in result.columns
        assert "studios" in result.columns
        assert result.iloc[0]["title"] == "Cowboy Bebop"
        assert result.iloc[0]["studios"] == "Sunrise"
        assert result.iloc[0]["genres"] == "Action, Sci-Fi"

    def test_convert_empty_jikan_data(self):
        """Should return empty DataFrame for empty input."""
        result = convert_jikan_to_dataframe([])
        assert len(result) == 0

    def test_convert_jikan_missing_fields(self):
        """Should handle missing optional fields."""
        jikan_data = [
            {
                "mal_id": 1,
                "title": "Test Anime",
                "synopsis": None,  # Missing
                "score": None,  # Missing
                "scored_by": 0,
                "studios": [],  # Empty
                "genres": [],  # Empty
            }
        ]

        result = convert_jikan_to_dataframe(jikan_data)
        assert len(result) == 1
        # Should handle missing/empty


# ========== TESTS CONVERT ANILIST ==========


class TestConvertAnilistToDataframe:
    """Tests for convert_anilist_to_dataframe function."""

    def test_convert_valid_anilist_data(self):
        """Should convert AniList GraphQL response to DataFrame."""
        # ARRANGE - Mock AniList GraphQL response
        anilist_data = {
            "data": {
                "Page": {
                    "media": [
                        {
                            "id": 1,
                            "idMal": 1,
                            "title": {
                                "romaji": "Cowboy Bebop",
                                "english": "Cowboy Bebop",
                            },
                            "averageScore": 86,
                            "trending": 50,
                        },
                        {
                            "id": 2,
                            "idMal": 20,
                            "title": {"romaji": "Naruto", "english": "Naruto"},
                            "averageScore": 78,
                            "trending": 30,
                        },
                    ]
                }
            }
        }

        # ACT
        result = convert_anilist_to_dataframe(anilist_data)

        # ASSERT
        assert len(result) == 2
        assert "id" in result.columns
        assert "idMal" in result.columns
        assert "title_romaji" in result.columns
        assert "title_english" in result.columns
        assert "averageScore" in result.columns
        assert result.iloc[0]["title_romaji"] == "Cowboy Bebop"
        assert result.iloc[0]["averageScore"] == 86

    def test_convert_anilist_missing_english_title(self):
        """Should handle missing English titles."""
        anilist_data = {
            "data": {
                "Page": {
                    "media": [
                        {
                            "id": 1,
                            "idMal": 1,
                            "title": {
                                "romaji": "Anime Japonais",
                                "english": None,  # Pas de titre anglais
                            },
                            "averageScore": 80,
                            "trending": 20,
                        }
                    ]
                }
            }
        }

        result = convert_anilist_to_dataframe(anilist_data)
        assert len(result) == 1
        assert result.iloc[0]["title_romaji"] == "Anime Japonais"
        assert pd.isna(result.iloc[0]["title_english"])

    def test_convert_empty_anilist_data(self):
        """Should return empty DataFrame for empty response."""
        anilist_data = {"data": {"Page": {"media": []}}}

        result = convert_anilist_to_dataframe(anilist_data)
        assert len(result) == 0

    def test_convert_anilist_no_media(self):
        """Should handle response with no media field."""
        anilist_data = {"data": {"Page": {}}}

        result = convert_anilist_to_dataframe(anilist_data)
        assert len(result) == 0


# ========== TESTS CALCULATE SCORES ==========


class TestCalculateAggregatedScores:
    """Tests for calculate_aggregated_scores function."""

    def test_scores_with_all_sources(self):
        """Should calculate average from all three sources."""
        # ARRANGE
        df_kaggle = pd.DataFrame({"anime_id": [1, 2], "rating": [8.0, 7.5]})

        df_jikan = pd.DataFrame({"mal_id": [1, 2], "score": [8.5, 7.8]})

        df_anilist = pd.DataFrame(
            {
                "idMal": [1],
                "averageScore": [85],  # 85/10 = 8.5
            }
        )

        # ACT
        result = calculate_aggregated_scores(df_kaggle, df_jikan, df_anilist)

        # ASSERT
        assert len(result) == 2
        assert "anime_id" in result.columns
        assert "mal_score" in result.columns
        assert "anilist_score" in result.columns
        assert "avg_score" in result.columns

        # Anime 1: has all 3 sources
        anime_1 = result[result["anime_id"] == 1].iloc[0]
        assert anime_1["mal_score"] == 8.5  # Jikan prioritaire
        assert anime_1["anilist_score"] == 8.5  # 85/10
        assert anime_1["avg_score"] == 8.5  # (8.5 + 8.5) / 2

        # Anime 2: only Kaggle + Jikan
        anime_2 = result[result["anime_id"] == 2].iloc[0]
        assert anime_2["mal_score"] == 7.8
        assert pd.isna(anime_2["anilist_score"])
        assert anime_2["avg_score"] == 7.8  # Only Jikan score

    def test_scores_jikan_priority_over_kaggle(self):
        """Jikan score should take priority over Kaggle for mal_score."""
        df_kaggle = pd.DataFrame(
            {
                "anime_id": [1],
                "rating": [7.0],  # Kaggle score
            }
        )

        df_jikan = pd.DataFrame(
            {
                "mal_id": [1],
                "score": [8.5],  # Jikan score (plus précis)
            }
        )

        df_anilist = pd.DataFrame(columns=["idMal", "averageScore"])

        result = calculate_aggregated_scores(df_kaggle, df_jikan, df_anilist)

        # Jikan score should be used (not Kaggle)
        assert result.iloc[0]["mal_score"] == 8.5

    def test_scores_kaggle_fallback(self):
        """Should use Kaggle score if Jikan not available."""
        df_kaggle = pd.DataFrame({"anime_id": [1, 2], "rating": [7.0, 8.0]})

        df_jikan = pd.DataFrame(
            {
                "mal_id": [1],  # Only anime 1
                "score": [8.5],
            }
        )

        df_anilist = pd.DataFrame(columns=["idMal", "averageScore"])

        result = calculate_aggregated_scores(df_kaggle, df_jikan, df_anilist)

        # Anime 1: Jikan score
        assert result[result["anime_id"] == 1].iloc[0]["mal_score"] == 8.5

        # Anime 2: Kaggle fallback
        assert result[result["anime_id"] == 2].iloc[0]["mal_score"] == 8.0

    def test_scores_anilist_normalization(self):
        """AniList scores should be normalized from 0-100 to 0-10."""
        df_kaggle = pd.DataFrame({"anime_id": [1], "rating": [8.0]})

        df_jikan = pd.DataFrame(columns=["mal_id", "score"])

        df_anilist = pd.DataFrame(
            {
                "idMal": [1],
                "averageScore": [75],  # 75/10 = 7.5
            }
        )

        result = calculate_aggregated_scores(df_kaggle, df_jikan, df_anilist)

        assert result.iloc[0]["anilist_score"] == 7.5

    def test_scores_empty_sources(self):
        """Should handle empty DataFrames."""
        df_kaggle = pd.DataFrame({"anime_id": [1], "rating": [8.0]})

        df_jikan = pd.DataFrame(columns=["mal_id", "score"])
        df_anilist = pd.DataFrame(columns=["idMal", "averageScore"])

        result = calculate_aggregated_scores(df_kaggle, df_jikan, df_anilist)

        assert len(result) == 1
        assert result.iloc[0]["mal_score"] == 8.0
        assert pd.isna(result.iloc[0]["anilist_score"])


# ========== TESTS FUZZY MATCH ==========


class TestFuzzyMatchTitles:
    """Tests for fuzzy_match_titles function."""

    def test_match_identical_titles(self):
        """Should find exact matches between DataFrames."""
        # ARRANGE
        df1 = pd.DataFrame({"name": ["Cowboy Bebop", "Naruto"]})

        df2 = pd.DataFrame({"title": ["Cowboy Bebop", "One Piece"]})

        # ACT
        result = fuzzy_match_titles(df1, df2, "name", "title", threshold=95)

        # ASSERT
        assert len(result) == 1
        assert result.iloc[0]["title_df1"] == "Cowboy Bebop"
        assert result.iloc[0]["title_df2"] == "Cowboy Bebop"
        assert result.iloc[0]["score"] == 100

    def test_match_fuzzy_titles(self):
        """Should find fuzzy matches with typos."""
        df1 = pd.DataFrame({"name": ["Cowboy Bebop"]})

        df2 = pd.DataFrame(
            {
                "title": ["Cowboy Bepop"]  # Typo
            }
        )

        result = fuzzy_match_titles(df1, df2, "name", "title", threshold=85)

        # Should match despite typo (score ~92)
        assert len(result) >= 1
        assert result.iloc[0]["score"] >= 85

    def test_match_no_matches(self):
        """Should return empty DataFrame when no matches."""
        df1 = pd.DataFrame({"name": ["Cowboy Bebop"]})

        df2 = pd.DataFrame({"title": ["Dragon Ball Z"]})

        result = fuzzy_match_titles(df1, df2, "name", "title", threshold=90)

        assert len(result) == 0

    def test_match_empty_dataframes(self):
        """Should handle empty DataFrames."""
        df1 = pd.DataFrame(columns=["name"])
        df2 = pd.DataFrame(columns=["title"])

        result = fuzzy_match_titles(df1, df2, "name", "title")

        assert len(result) == 0
