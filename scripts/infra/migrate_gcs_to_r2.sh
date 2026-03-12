#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# migrate_gcs_to_r2.sh — Migrate properlytic-raw-data from GCS → Cloudflare R2
#
# Run this from a GCP Cloud Shell or VM to avoid egress charges.
# Prerequisites: rclone installed (curl https://rclone.org/install.sh | sudo bash)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
GCS_BUCKET="properlytic-raw-data"
R2_BUCKET="properlytic-raw-data"
R2_ACCOUNT_ID="7f58e07bff423d2120acf10aa6bf7a32"
R2_ACCESS_KEY_ID="e6e1afa63a6e7adab7b028f56ed93ef5"
R2_SECRET_ACCESS_KEY="ebbfa6c05be0947bd54b81dee66bc44c569ed3fadf82f85ce9c58bcc97e09e88"

# ── Set up rclone remotes ─────────────────────────────────────────────────────
echo "📋 Configuring rclone remotes..."

# GCS remote (uses Application Default Credentials in Cloud Shell)
rclone config create gcs-source "google cloud storage" \
    bucket_policy_only true \
    --non-interactive 2>/dev/null || true

# R2 remote (S3-compatible)
rclone config create r2-dest s3 \
    provider Cloudflare \
    access_key_id "$R2_ACCESS_KEY_ID" \
    secret_access_key "$R2_SECRET_ACCESS_KEY" \
    endpoint "https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com" \
    acl private \
    --non-interactive 2>/dev/null || true

echo "✅ Remotes configured"

# ── Dry-run first ─────────────────────────────────────────────────────────────
echo ""
echo "📊 Listing source bucket (GCS)..."
rclone size "gcs-source:${GCS_BUCKET}" 2>/dev/null || echo "⚠️  Could not get size. Proceeding anyway."

echo ""
echo "🔍 Running dry-run sync (no data transferred)..."
rclone sync "gcs-source:${GCS_BUCKET}" "r2-dest:${R2_BUCKET}" \
    --dry-run \
    --progress \
    --fast-list \
    --transfers 16 \
    --checkers 32 \
    2>&1 | tail -5

# ── Prompt before actual sync ─────────────────────────────────────────────────
echo ""
read -p "Ready to sync 194 GB from GCS → R2? (y/N) " confirm
if [[ "${confirm,,}" != "y" ]]; then
    echo "❌ Aborted."
    exit 0
fi

# ── Actual sync ───────────────────────────────────────────────────────────────
echo ""
echo "🚀 Starting sync: GCS → R2 (this may take a while)..."
rclone sync "gcs-source:${GCS_BUCKET}" "r2-dest:${R2_BUCKET}" \
    --progress \
    --fast-list \
    --transfers 16 \
    --checkers 32 \
    --stats 30s \
    --log-file /tmp/rclone_migration.log \
    --log-level INFO

echo ""
echo "✅ Migration complete!"
echo ""

# ── Verify ────────────────────────────────────────────────────────────────────
echo "📊 Verifying R2 bucket size..."
rclone size "r2-dest:${R2_BUCKET}"

echo ""
echo "📋 Log file: /tmp/rclone_migration.log"
echo ""
echo "Next steps:"
echo "  1. Verify object counts match between GCS and R2"
echo "  2. Update Vercel env vars (R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET)"
echo "  3. Update Modal secrets with the same R2 env vars"
echo "  4. Deploy the updated app code"
echo "  5. Monitor for 1 week, then decommission GCS bucket"
