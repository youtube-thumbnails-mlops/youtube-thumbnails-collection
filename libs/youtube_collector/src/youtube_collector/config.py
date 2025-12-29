"""Simple configuration for YouTube Collector."""

import os
from typing import Optional


class ConfigError(Exception):
    """Raised when API key is missing."""
    pass


def get_api_key(api_key: Optional[str] = None) -> str:
    """
    Get YouTube API key.

    Args:
        api_key: Optional API key passed directly

    Returns:
        API key string

    Raises:
        ConfigError: If no API key found
    """
    if api_key:
        return api_key.strip()

    env_key = os.getenv("YOUTUBE_API_KEY")
    if env_key:
        return env_key.strip()

    raise ConfigError(
        "YouTube API key not found. Set YOUTUBE_API_KEY environment variable "
        "or pass api_key parameter. "
    )


def get_output_dir(default: str = "./data/thumbnails") -> str:
    """Get output directory for thumbnails."""
    return os.getenv("OUTPUT_DIR", default)