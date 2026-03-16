"""
Centralized configuration for the ETL pipeline.
Loads environment variables and provides configuration access.
"""

import os
from typing import Any, Dict

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def load_config() -> Dict[str, Any]:
    """
    Load environment variables from .env file.

    Note: Default values are provided for local development.
    In production, sensitive values (passwords, API keys) should
    NEVER have defaults and must be explicitly set in .env

    Returns:
        Dict containing all project configuration
    """
    load_dotenv()

    return {
        "db_host": os.getenv("DB_HOST", "postgres"),
        "db_port": int(os.getenv("DB_PORT", "5432")),
        "db_name": os.getenv("DB_NAME", "anime_db"),  # Dev only
        "db_user": os.getenv("DB_USER", "anime_user"),  # Dev only
        "db_password": os.getenv("DB_PASSWORD", "anime_password"),  # Dev only
        "jikan_api_base_url": os.getenv(
            "JIKAN_API_BASE_URL", "https://api.jikan.moe/v4"
        ),  # Dev only
        "anilist_api_url": os.getenv(
            "ANILIST_API_URL", "https://graphql.anilist.co"
        ),  # Dev only
        "data_raw_path": os.getenv("DATA_RAW_PATH", "data/raw"),
        "data_processed_path": os.getenv("DATA_PROCESSED_PATH", "data/processed"),
        "jikan_rate_limit_rps": int(os.getenv("JIKAN_RATE_LIMIT_RPS", "3")),
        "anilist_rate_limit_rpm": int(os.getenv("ANILIST_RATE_LIMIT_RPM", "90")),
        "log_level": os.getenv("LOG_LEVEL", "INFO"),
    }


def get_db_engine() -> Engine:
    """
    Create and return a SQLAlchemy engine for PostgreSQL.

    Returns:
        SQLAlchemy Engine connected to PostgreSQL
    """
    config = load_config()

    # Build connection string using config values
    connection_string = (
        f"postgresql+psycopg2://{config['db_user']}:{config['db_password']}"
        f"@{config['db_host']}:{config['db_port']}/{config['db_name']}"
    )

    # Create and return engine
    engine = create_engine(connection_string)
    return engine
