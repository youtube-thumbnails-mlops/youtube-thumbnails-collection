"""
TEST MODE: Daily data collection script.
Configured for minimal downloads and fast rotation.
Includes W&B logging with resized images.
"""
import os
import sys
import wandb
from pathlib import Path
import math
from PIL import Image
from youtube_collector import YouTubeClient

# --- TEST CONFIGURATION ---
BATCH_LIMIT = 3          # Rotate after just 3 images!
TEST_CATEGORY = ['20']   # Only search 'Gaming' (saves 90% quota)
TEST_REGION = "US"       # Only search 'US' (saves 90% quota)
MAX_WANDB_RUNS = 2       # Match test MAX_BATCHES for consistency
# --------------------------

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
                name_no_ext = dvc_file.stem
                num = int(name_no_ext.replace("batch_", ""))
                versions.append(num)
            except ValueError:
                continue
        if versions:
            return max(versions) + 1
    return 1

def calculate_metrics(video):
    # Calculate baseline (Average views per video for this channel)
    total_views = float(video.get('channel_total_views', 0))
    video_count = max(float(video.get('channel_video_count', 1)), 1.0)
    avg_views = total_views / video_count
    
    # Calculate Log-Difference
    log_views = math.log10(float(video.get('views', 0)) + 1)
    log_baseline = math.log10(avg_views + 1)
    
    return log_views - log_baseline

def prune_old_wandb_runs(project_name, max_runs):
    """Deletes oldest runs to keep W&B storage inside the 5GB limit."""
    try:
        api = wandb.Api()
        runs = api.runs(path=f"{api.default_entity}/{project_name}")
        if len(runs) > max_runs:
            runs_to_delete = len(runs) - max_runs
            print(f"🧹 W&B Pruning: Found {len(runs)} runs. Deleting {runs_to_delete} oldest...")
            sorted_runs = sorted(runs, key=lambda run: run.created_at)
            for i in range(runs_to_delete):
                print(f"   - Deleting run: {sorted_runs[i].name}")
                sorted_runs[i].delete()
            print("✅ W&B Pruning Complete.")
    except Exception as e:
        print(f"⚠️ W&B Pruning failed: {e}")

def main():
    client = YouTubeClient()

    current_dir = Path("current")
    batches_dir = Path("batches")
    current_dir.mkdir(exist_ok=True)

    # 1. Setup Versioning
    next_batch_num = get_next_batch_number(batches_dir)
    target_batch_name = f"batch_{next_batch_num:03d}"
    print(f"🎯 Target Version: {target_batch_name}")

    # 2. Fetch minimal videos (Low Quota Usage)
    print("🧪 RUNNING IN TEST MODE")
    print("Fetching ~1-2 videos...")

    videos = client.fetch_batch(
        days_ago=7,                  # Search past week (more likely to find videos)
        videos_per_category=2,       # Try to get 2 videos
        categories=TEST_CATEGORY,    # Only 1 category (Gaming)
        region=TEST_REGION,          # Only 1 region (US)
        min_subscribers=100,         # Very low barrier for test
        min_views=10,                # Very low views requirement
        min_duration_seconds=30,     # Lower duration requirement
        video_duration="medium",
    )

    if not videos:
        print("No videos found. (This happens sometimes in strict test mode).")
        sys.exit(0)

    # 3. Download
    print(f"\n⬇️ Downloading {len(videos)} thumbnails to current/...")
    client.download_thumbnails_bulk(
        videos,
        output_dir=str(current_dir)
    )

    # 4. Save Metadata (Enriched with Metrics)
    for video in videos:
        ratio = calculate_metrics(video)
        video['viral_ratio'] = ratio
        video['batch_version'] = target_batch_name

    metadata_file = current_dir / "metadata.csv"
    client.save_to_csv(videos, filename=str(metadata_file))
    print(f"✓ Appended {len(videos)} videos")

    # 5. Log to W&B (Resized for Storage)
    try:
        print("🚀 Logging to Weights & Biases (Compressed)...")
        wandb.login(key=os.getenv("WANDB_API_KEY"))

        run = wandb.init(
            project="youtube-thumbnails-dataset",
            job_type="test_collection",
            tags=["test", "daily", target_batch_name],
            config={"batch_version": target_batch_name, "mode": "test"}
        )

        table = wandb.Table(columns=[
            "thumbnail", "video_id", "title", "category_id", "category_name",
            "views", "likes", "comments",
            "channel_id", "channel_subscribers", "channel_total_views", "channel_video_count",
            "tags", "description_len", "duration_seconds", "definition", "language",
            "published_at", "captured_at", "video_url", "thumbnail_url",
            "viral_ratio", "batch_version"
        ])

        for video in videos:
            img_path = current_dir / f"{video['video_id']}.jpg"
            if img_path.exists():
                # Metrics already in video dict

                # --- RESIZING MAGIC ---
                with Image.open(img_path) as im:
                    # Convert to RGB
                    im = im.convert('RGB')
                    # Resize to fit within a 400x400 box, MAINTAINING aspect ratio.
                    # A typical 16:9 thumbnail will become roughly 400x225 pixels.
                    # This happens in RAM, doesn't touch the file on disk.
                    im.thumbnail((400, 400))

                    table.add_data(
                        wandb.Image(im),
                        video['video_id'], video['title'], video['category_id'], video['category_name'],
                        video['views'], video['likes'], video['comments'],
                        video['channel_id'], video['channel_subscribers'],
                        video['channel_total_views'], video['channel_video_count'],
                        video['tags'], video['description_len'], video['duration_seconds'],
                        video['definition'], video['language'],
                        video['published_at'], video['captured_at'],
                        video['video_url'], video['thumbnail_url'],
                        video['viral_ratio'], target_batch_name
                    )
                # ----------------------

        wandb.log({"collected_batch": table})
        wandb.finish()
        print("✅ W&B Logging Complete")

        # Prune W&B (Keep in sync with test MAX_BATCHES=2)
        prune_old_wandb_runs("youtube-thumbnails-dataset", max_runs=MAX_WANDB_RUNS)

    except Exception as e:
        print(f"⚠️ W&B Logging/Pruning failed: {e}")

    # 6. Check Rotation
    total = count_samples(metadata_file)
    print(f"📊 Total in current/: {total}/{BATCH_LIMIT}")

    if total >= BATCH_LIMIT:
        next_batch = get_next_batch_number(batches_dir)
        rotate_flag = Path(".rotate")
        rotate_flag.write_text(f"batch_{next_batch:03d}")

        print(f"\n🔄 TEST ROTATION TRIGGERED")
        print(f"📝 Flag created: .rotate → batch_{next_batch:03d}")
    else:
        print(f"✅ Collection complete (No rotation yet)")

if __name__ == "__main__":
    main()
