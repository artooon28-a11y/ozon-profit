#!/bin/sh
# Pulls new commits from GitHub and rebuilds the site. Run by cron every 2 minutes.
set -e
cd "$(dirname "$0")/.."
git fetch -q origin main
[ "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)" ] && exit 0
echo "$(date '+%F %T') update $(git rev-parse --short HEAD) -> $(git rev-parse --short origin/main)"
# Caddy reads the mounted Caddyfile only at start, so a rebuild alone does not apply it.
caddy_changed=$(git diff --quiet HEAD origin/main -- Caddyfile || echo yes)
git merge -q --ff-only origin/main
docker compose up -d --build
[ -n "$caddy_changed" ] && docker compose restart caddy
docker image prune -f >/dev/null
echo "$(date '+%F %T') done"
