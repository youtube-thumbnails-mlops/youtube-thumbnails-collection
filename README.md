# YouTube Thumbnails MLOps

Automated data collection pipeline with batch rotation.

## How It Works

Daily at 8 AM UTC:
1. Pull `current/` folder (max 500 images)
2. Fetch and add new thumbnails (varies by filtering/availability)
3. If < 500: push and done
4. If >= 500: rotate `current/` → `batches/batch_XXX/`, create version tag

## Structure

```
youtube-thumbnails-monorepo/
├── libs/youtube_collector/    # Collection library
├── scripts/collect_daily.py   # Daily collection + rotation
└── .github/workflows/
    ├── ci.yml                 # PR tests
    └── daily_collect.yml      # Daily collection
```

## Setup

1. Install library:
```bash
pip install -e libs/youtube_collector
```

2. Configure GitHub secrets:
- `YOUTUBE_API_KEY`
- `DATASET_REPO_TOKEN`
- `R2_ENDPOINT`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`

3. Push and let it run daily

## Related

- [youtube-thumbnails-dataset](https://github.com/YOUR_ORG/youtube-thumbnails-dataset) - DVC-tracked data storage
