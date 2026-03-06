"""
Unit tests for loading module.
"""

import pytest
from sqlalchemy import create_engine

from src.load import create_schema, drop_schema


@pytest.fixture
def test_engine():
    """Create test database engine."""
    # Use in-memory SQLite for testing
    engine = create_engine("sqlite:///:memory:")
    yield engine
    engine.dispose()


class TestSchema:
    """Tests for schema creation and deletion."""

    def test_create_schema(self, test_engine):
        """Test schema creation."""
        # Note: This will fail with SQLite (PostgreSQL-specific)
        # TODO: Mock or use test PostgreSQL instance
        pass


# TODO: Add more tests
