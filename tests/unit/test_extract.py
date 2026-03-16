"""
Unit tests for extraction module.
"""

from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest
import requests

from src.extract import (
    extract_anilist_graphql,
    extract_jikan_api,
    extract_kaggle_csv,
    should_retry_http_error,
)


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


class TestExtractJikanApi:
    """Tests for extract_jikan_api function."""

    def test_extract_jikan_successful(self, mocker):
        """Should extract anime data from Jikan API."""
        # ARRANGE - Mock requests.get
        mock_get = mocker.patch("src.extract.requests.get")

        # Définir ce que l'API retourne (fake response)
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": {
                "mal_id": 1,
                "title": "Cowboy Bebop",
                "synopsis": "In the year 2071...",
                "score": 8.78,
                "scored_by": 500000,
                "studios": [{"name": "Sunrise"}],
                "genres": [{"name": "Action"}, {"name": "Sci-Fi"}],
            }
        }
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        # ACT - Appelle la fonction (qui utilisera le mock)
        result = extract_jikan_api(anime_ids=[1], base_url="https://api.jikan.moe/v4")

        # ASSERT - Vérifie le résultat
        assert len(result) == 1
        assert result[0]["mal_id"] == 1
        assert result[0]["title"] == "Cowboy Bebop"
        assert result[0]["studios"][0]["name"] == "Sunrise"

        # Vérifie que requests.get a été appelé
        mock_get.assert_called_once()

    def test_extract_jikan_multiple_animes(self, mocker):
        """Should extract multiple animes from Jikan API."""
        mock_get = mocker.patch("src.extract.requests.get")

        # Mock retourne différentes réponses selon l'ID
        def mock_response_factory(url, *args, **kwargs):
            response = Mock()
            response.status_code = 200

            if "anime/1" in url:
                response.json.return_value = {
                    "data": {"mal_id": 1, "title": "Cowboy Bebop", "score": 8.78}
                }
            elif "anime/2" in url:
                response.json.return_value = {
                    "data": {"mal_id": 2, "title": "Naruto", "score": 8.23}
                }

            return response

        mock_get.side_effect = mock_response_factory

        # ACT
        result = extract_jikan_api(
            anime_ids=[1, 2], base_url="https://api.jikan.moe/v4"
        )

        # ASSERT
        assert len(result) == 2
        assert result[0]["title"] == "Cowboy Bebop"
        assert result[1]["title"] == "Naruto"
        assert mock_get.call_count == 2

    def test_extract_jikan_empty_list(self, mocker):
        """Should return empty list for empty anime_ids."""
        mock_get = mocker.patch("src.extract.requests.get")

        result = extract_jikan_api(anime_ids=[], base_url="https://api.jikan.moe/v4")

        assert result == []
        mock_get.assert_not_called()


class TestExtractAnilistGraphql:
    """Tests for extract_anilist_graphql function."""

    def test_extract_anilist_successful(self, mocker):
        """Should extract anime data from AniList GraphQL API."""
        # ARRANGE - Mock requests.post
        mock_post = mocker.patch("src.extract.requests.post")

        # Fake GraphQL response
        mock_response = Mock()
        mock_response.json.return_value = {
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
                        }
                    ]
                }
            }
        }
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        # ACT
        query = "query { Page { media { id } } }"
        variables = {"page": 1, "perPage": 50}

        result = extract_anilist_graphql(
            query=query, variables=variables, api_url="https://graphql.anilist.co"
        )

        # ASSERT
        assert "data" in result
        assert "Page" in result["data"]
        assert len(result["data"]["Page"]["media"]) == 1
        assert result["data"]["Page"]["media"][0]["id"] == 1

        # Vérifie que requests.post a été appelé avec les bons arguments
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert call_args[1]["json"]["query"] == query
        assert call_args[1]["json"]["variables"] == variables

    def test_extract_anilist_empty_response(self, mocker):
        """Should handle empty media list from AniList."""
        mock_post = mocker.patch("src.extract.requests.post")

        mock_response = Mock()
        mock_response.json.return_value = {"data": {"Page": {"media": []}}}
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        query = "query { Page { media { id } } }"
        result = extract_anilist_graphql(
            query=query, variables={}, api_url="https://graphql.anilist.co"
        )

        assert result["data"]["Page"]["media"] == []

    def test_extract_anilist_http_error(self, mocker):
        """Should handle HTTP errors from AniList API."""
        mock_post = mocker.patch("src.extract.requests.post")

        # Simulate HTTP error
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = Exception("Server Error")
        mock_post.return_value = mock_response

        query = "query { Page { media { id } } }"

        # Should raise exception or return empty
        with pytest.raises(Exception):
            result = extract_anilist_graphql(
                query=query, variables={}, api_url="https://graphql.anilist.co"
            )


class TestShouldRetryHttpError:
    """Tests for should_retry_http_error helper function."""

    def test_retry_on_429_too_many_requests(self):
        """Should retry on 429 (rate limit) errors."""
        error = requests.exceptions.HTTPError()
        error.response = Mock()
        error.response.status_code = 429

        assert should_retry_http_error(error) is True

    def test_retry_on_500_server_error(self):
        """Should retry on 500 (server) errors."""
        error = requests.exceptions.HTTPError()
        error.response = Mock()
        error.response.status_code = 500

        assert should_retry_http_error(error) is True

    def test_retry_on_502_bad_gateway(self):
        """Should retry on 502 errors."""
        error = requests.exceptions.HTTPError()
        error.response = Mock()
        error.response.status_code = 502

        assert should_retry_http_error(error) is True

    def test_retry_on_503_service_unavailable(self):
        """Should retry on 503 errors."""
        error = requests.exceptions.HTTPError()
        error.response = Mock()
        error.response.status_code = 503

        assert should_retry_http_error(error) is True

    def test_no_retry_on_404_not_found(self):
        """Should NOT retry on 404 errors."""
        error = requests.exceptions.HTTPError()
        error.response = Mock()
        error.response.status_code = 404

        assert should_retry_http_error(error) is False

    def test_no_retry_on_400_bad_request(self):
        """Should NOT retry on 400 errors."""
        error = requests.exceptions.HTTPError()
        error.response = Mock()
        error.response.status_code = 400

        assert should_retry_http_error(error) is False

    def test_no_retry_on_non_http_error(self):
        """Should NOT retry on non-HTTP errors."""
        error = ValueError("Some other error")

        assert should_retry_http_error(error) is False

    def test_no_retry_on_error_without_response(self):
        """Should NOT retry if error has no response attribute."""
        error = requests.exceptions.RequestException()

        assert should_retry_http_error(error) is False
