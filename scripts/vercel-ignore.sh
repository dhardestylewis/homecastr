#!/bin/bash

# Vercel Ignore Command
# Returns 0 to CANCEL the build, returning 1 to PROCEED with the build.

# 1. Always cancel Vercel's automatic Git Integration builds on the main branch,
# because we already have a GitHub Action (.github/workflows/deploy.yml) that builds
# main and deploys using `vercel deploy --prebuilt`.
if [[ "$VERCEL_GIT_COMMIT_REF" == "main" ]]; then
  echo "Branch is main. Canceling Vercel build (GitHub Actions will handle this via --prebuilt)."
  exit 0
fi

# 2. For Preview builds (PRs, other branches), only build if frontend files have changed.
# git diff HEAD^ HEAD --quiet returns 0 if NO changes, 1 if changes exist.
# We check if there are any changes in the directories that affect the frontend.

if git diff HEAD^ HEAD --quiet .github .meta _scratch docs notebooks scripts sql supabase Prototyping_Urban_Tech .env* *.md; then
  # No changes in the backend folders, which means only frontend changed (or nothing changed).
  # Wait, the better way is to explicitly check frontend directories to proceed.
  :
fi

# A safer approach: Proceed with build if any of these frontend core paths changed.
git diff HEAD^ HEAD --quiet app components lib public styles hooks middleware.ts next.config.mjs package.json pnpm-lock.yaml vercel.json components.json tsconfig.json

# If git diff returns 1 (changes found), we exit 1 to PROCEED with the build.
# If git diff returns 0 (no changes), we exit 0 to CANCEL the build.
if [ $? -eq 1 ]; then
  echo "Frontend files changed. Proceeding with build."
  exit 1
else
  echo "Only backend/ignored files changed. Canceling build."
  exit 0
fi
