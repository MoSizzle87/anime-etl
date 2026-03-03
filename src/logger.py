"""
Logging configuration for the anime ETL pipeline.
"""

import logging
import logging.config
from pathlib import Path

import yaml


def setup_logging(config_path: str = "logging_config.yaml") -> logging.Logger:
    """
    Setup logging from YAML config file.

    Args:
        config_path: Path to logging config YAML

    Returns:
        Logger instance for the pipeline
    """
    # Create logs directory if needed
    Path("logs").mkdir(exist_ok=True)

    # Load config
    config_file = Path(config_path)
    if config_file.exists():
        with open(config_file) as f:
            config = yaml.safe_load(f)
        logging.config.dictConfig(config)
    else:
        # Fallback to basic config
        logging.basicConfig(level=logging.INFO, format="%(message)s")

    return logging.getLogger("src")


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s"
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m"
