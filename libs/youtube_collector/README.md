# YouTube Collector

Python library for collecting YouTube thumbnails.

## Install

```bash
pip install -e .
```

## Usage

```python
from youtube_collector import YouTubeClient

client = YouTubeClient()
videos = client.fetch_balanced_dataset(days_ago=7, videos_per_category=5)
client.download_thumbnails_bulk(videos, output_dir="current")
```

## API Key

```bash
export YOUTUBE_API_KEY=your_key
```
