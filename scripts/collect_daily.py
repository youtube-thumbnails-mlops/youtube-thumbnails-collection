"""
Daily data collection script.
Collects thumbnails and appends to current/ staging area.
Signals rotation needed via .rotate flag file.
"""
import os
import csv
import sys
from pathlib import Path
from youtube_collector import YouTubeClient

# CONSTANTS
BATCH_LIMIT = 500

def count_samples(metadata_file):
    """Count total samples in metadata CSV."""
    if not metadata_file.exists():
        return 0
    with open(metadata_file, 'r', encoding='utf-8') as f:
        return sum(1 for _ in f) - 1  # Subtract header

def get_next_batch_number(batches_dir):
    """Find the next batch number by counting .dvc files."""
    batches_dir.mkdir(exist_ok=True)
    existing_dvc_files = list(batches_dir.glob("batch_*.dvc"))
    
    if existing_dvc_files:
        versions = []
        for dvc_file in existing_dvc_files:
            try:
                name_no_ext = dvc_file.stem  # "batch_001"
                num = int(name_no_ext.replace("batch_", ""))
                versions.append(num)
            except ValueError:
                continue
        if versions:
            return max(versions) + 1
    return 1

def main():
    client = YouTubeClient()

    current_dir = Path("current")
    batches_dir = Path("batches")
    current_dir.mkdir(exist_ok=True)

    # 1. Fetch new videos (UPDATED to match your new client.py)
    print("Fetching videos for daily collection...")
    videos = client.fetch_batch(     # <--- Changed from fetch_balanced_dataset
        days_ago=7,
        videos_per_category=5,
        region="US_EU",              # Will be randomized inside the client
        min_subscribers=10000,
        min_views=100,
        video_duration="medium",
    )

    if not videos:
        print("No videos found today. Exiting.")
        sys.exit(0)

    # 2. Download thumbnails (UPDATED: removed include_label argument)
    print(f"\n⬇️ Downloading {len(videos)} thumbnails to current/...")
    client.download_thumbnails_bulk(
        videos,
        output_dir=str(current_dir)
    )

    # 3. Append to current/metadata.csv
    metadata_file = current_dir / "metadata.csv"
    
    # Use the library's built-in save function to handle headers correctly
    client.save_to_csv(
        videos, 
        filename=str(metadata_file)
    )

    print(f"✓ Appended {len(videos)} videos to {metadata_file}")

    # 4. Check if rotation needed
    total = count_samples(metadata_file)
    print(f"📊 Total in current/: {total}/{BATCH_LIMIT} samples")

    if total >= BATCH_LIMIT:
        # Signal rotation needed to workflow
        next_batch = get_next_batch_number(batches_dir)
        rotate_flag = Path(".rotate")
        rotate_flag.write_text(f"batch_{next_batch:03d}")
        
        print(f"\n🔄 ROTATION NEEDED: {total} samples")
        print(f"📝 Flag created: .rotate → batch_{next_batch:03d}")
        print(f"⚠️  Workflow will handle DVC rotation")
    else:
        print(f"✅ Daily collection complete")

if __name__ == "__main__":
    main()