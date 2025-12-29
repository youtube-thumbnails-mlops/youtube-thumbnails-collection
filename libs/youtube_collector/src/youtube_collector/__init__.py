"""
YouTube Collector Library

A library for collecting video data from YouTube using the YouTube Data API v3.
"""

__version__ = "0.1.0"

from .client import YouTubeClient
from .config import ConfigError

__all__ = [
    "YouTubeClient",
    "ConfigError",
]
