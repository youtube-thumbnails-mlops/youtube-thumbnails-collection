#!/bin/bash
# Reset Dataset to Fresh State
# This script:
# 1. Removes all local data (current/, batches/, .dvc/cache)
# 2. Removes all git tags
# 3. Resets dataset repo to clean commit
# 4. Cleans R2 bucket completely
# 5. Cleans W&B runs completely
# 6. Force pushes clean state to GitHub

set -e  # Exit on error

# Navigate to dataset repo (relative to this script in monorepo)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATASET_DIR="$SCRIPT_DIR/../../youtube-thumbnails-dataset"

echo "🧹 RESETTING DATASET TO FRESH STATE"
echo "===================================="
echo ""

# Make sure we're in the dataset repo
cd "$DATASET_DIR"

# 1. Remove local data
echo "📂 Removing local data..."
rm -rf current batches test_set .dvc/cache
echo "   ✅ Local data removed"
echo ""

# 2. Remove all tags (local and remote)
echo "🏷️  Removing all tags..."
# Get all tags
TAGS=$(git tag)
if [ -n "$TAGS" ]; then
    # Delete local tags
    git tag -d $TAGS 2>/dev/null || true

    # Delete remote tags
    for tag in $TAGS; do
        git push origin :refs/tags/$tag 2>/dev/null || true
    done
    echo "   ✅ All tags removed"
else
    echo "   ℹ️  No tags to remove"
fi
echo ""

# 3. Reset to clean commit
echo "⏪ Resetting git to clean state..."
CLEAN_COMMIT="e9efe25"  # "Remove current.dvc for fresh start"
git reset --hard $CLEAN_COMMIT
echo "   ✅ Reset to commit $CLEAN_COMMIT"
echo ""

# 4. Clean R2 bucket
echo "🗑️  Cleaning R2 bucket..."

# Check if venv exists
if [ ! -d "venv" ]; then
    echo "   ⚠️  Virtual environment not found. Creating one..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -q boto3
else
    source venv/bin/activate
    # Install boto3 if not already installed
    pip show boto3 >/dev/null 2>&1 || python3 -m pip install -q boto3
fi

python3 << 'PYTHON_EOF'
import boto3
import os

# Load credentials from .env
env_vars = {}
if os.path.exists('.env'):
    with open('.env', 'r') as f:
        for line in f:
            if '=' in line and not line.startswith('#'):
                key, value = line.strip().split('=', 1)
                env_vars[key] = value
else:
    print("   ⚠️  .env file not found. Skipping R2 cleanup.")
    exit(0)

endpoint = env_vars.get('R2_ENDPOINT', '')
access_key = env_vars.get('R2_ACCESS_KEY_ID', '')
secret_key = env_vars.get('R2_SECRET_ACCESS_KEY', '')

if not all([endpoint, access_key, secret_key]):
    print("   ⚠️  R2 credentials not found in .env. Skipping R2 cleanup.")
    exit(0)

s3 = boto3.client(
    's3',
    endpoint_url=endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

bucket = 'youtube-thumbnails-dataset'

print(f"   Deleting all objects from {bucket}...")

try:
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket)

    deleted_count = 0
    for page in pages:
        if 'Contents' in page:
            objects = [{'Key': obj['Key']} for obj in page['Contents']]
            if objects:
                s3.delete_objects(Bucket=bucket, Delete={'Objects': objects})
                deleted_count += len(objects)
                print(f"   Deleted {len(objects)} objects...")

    print(f"   ✅ Total deleted: {deleted_count} objects")
except Exception as e:
    print(f"   ⚠️  Error cleaning R2: {e}")
PYTHON_EOF

deactivate
echo ""

# 5. Clean W&B runs
echo "🧹 Cleaning W&B runs..."

# Install wandb if not already installed
# Reuse venv from R2 step
source venv/bin/activate
pip show wandb >/dev/null 2>&1 || python3 -m pip install -q wandb

python3 << 'PYTHON_EOF'
import os

# Load WANDB_API_KEY from .env
env_vars = {}
if os.path.exists('.env'):
    with open('.env', 'r') as f:
        for line in f:
            if '=' in line and not line.startswith('#'):
                key, value = line.strip().split('=', 1)
                env_vars[key] = value
else:
    print("   ⚠️  .env file not found. Skipping W&B cleanup.")
    exit(0)

wandb_key = env_vars.get('WANDB_API_KEY', '')

if not wandb_key:
    print("   ⚠️  WANDB_API_KEY not found in .env. Skipping W&B cleanup.")
    exit(0)

try:
    import wandb
    wandb.login(key=wandb_key)

    api = wandb.Api()
    project_name = "youtube-thumbnails-dataset"
    runs = api.runs(path=f"{api.default_entity}/{project_name}")

    if len(runs) > 0:
        print(f"   Deleting {len(runs)} W&B runs...")
        for run in runs:
            print(f"   - Deleting run: {run.name}")
            run.delete()
        print(f"   ✅ Deleted {len(runs)} runs")
    else:
        print("   ℹ️  No W&B runs to delete")
except Exception as e:
    print(f"   ⚠️  W&B cleanup failed: {e}")
PYTHON_EOF

echo ""

# 6. Force push to GitHub
echo "🚀 Force pushing clean state to GitHub..."
read -p "   ⚠️  This will FORCE PUSH to GitHub. Continue? (y/N): " confirm
if [[ $confirm == [yY] ]]; then
    git push origin main --force --tags
    echo "   ✅ Pushed to GitHub"
else
    echo "   ⏭️  Skipped GitHub push (you can do it manually later with: git push origin main --force --tags)"
fi
echo ""

echo "✅ RESET COMPLETE!"
echo "=================="
echo ""
echo "Current state:"
echo "  - Local data: Cleaned"
echo "  - Git tags: Removed"
echo "  - Git commit: $CLEAN_COMMIT"
echo "  - R2 bucket: Empty"
echo "  - W&B runs: Deleted"
echo "  - GitHub: $(if [[ $confirm == [yY] ]]; then echo 'Updated'; else echo 'Not updated (manual push needed)'; fi)"
echo ""
echo "You can now trigger the workflow to start fresh!"
