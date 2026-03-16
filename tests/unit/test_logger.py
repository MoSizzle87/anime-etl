"""
Unit tests for logger module.
"""

from pathlib import Path

import pytest

from src.logger import format_duration, setup_logging


class TestFormatDuration:
    """Tests for format_duration function."""

    def test_format_seconds_only(self):
        """Should format duration under 1 minute."""
        assert format_duration(0) == "0.0s"
        assert format_duration(30) == "30.0s"
        assert format_duration(45.7) == "45.7s"
        assert format_duration(59.9) == "59.9s"

    def test_format_minutes_and_seconds(self):
        """Should format duration under 1 hour."""
        assert format_duration(60) == "1m 0s"
        assert format_duration(90) == "1m 30s"
        assert format_duration(125) == "2m 5s"
        assert format_duration(3599) == "59m 59s"

    def test_format_hours_and_minutes(self):
        """Should format duration over 1 hour."""
        assert format_duration(3600) == "1h 0m"
        assert format_duration(3661) == "1h 1m"
        assert format_duration(7325) == "2h 2m"

    def test_format_edge_cases(self):
        """Should handle edge cases."""
        assert format_duration(0.5) == "0.5s"
        assert format_duration(59.99) == "60.0s"


class TestSetupLogging:
    """Tests for setup_logging function."""

    def test_setup_with_valid_config(self, tmp_path):
        """Should setup logging with valid YAML config."""
        # Create a minimal YAML config
        config_file = tmp_path / "test_logging.yaml"
        config_file.write_text("""
version: 1
disable_existing_loggers: false
formatters:
  simple:
    format: '%(message)s'
handlers:
  console:
    class: logging.StreamHandler
    formatter: simple
root:
  level: INFO
  handlers: [console]
""")

        logger = setup_logging(str(config_file))

        assert logger is not None
        assert logger.name == "src"

    def test_setup_with_missing_config(self, tmp_path):
        """Should fallback to basicConfig if YAML not found."""
        missing_file = tmp_path / "nonexistent.yaml"

        logger = setup_logging(str(missing_file))

        assert logger is not None
        assert logger.name == "src"

    def test_creates_logs_directory(self, tmp_path, monkeypatch):
        """Should create logs directory if it doesn't exist."""
        # Change working directory to tmp_path
        monkeypatch.chdir(tmp_path)

        logger = setup_logging("nonexistent.yaml")

        assert (tmp_path / "logs").exists()
        assert logger is not None
