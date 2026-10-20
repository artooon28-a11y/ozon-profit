#!/bin/sh
# Pulls new commits from GitHub and rebuilds the site. Run by cron every 2 minutes.
set -e
cd "$(dirname "$0")/.."
git fetch -q origin main
[ "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)" ] && exit 0
echo "$(date '+%F %T') update $(git rev-parse --short HEAD) -> $(git rev-parse --short origin/main)"
git merge -q --ff-only origin/main
docker compose up -d --build
docker image prune -f >/dev/null
echo "$(date '+%F %T') done"
