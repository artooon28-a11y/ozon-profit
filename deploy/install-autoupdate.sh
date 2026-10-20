#!/bin/sh
# One-time setup: the server checks GitHub every 2 minutes and deploys new commits.
set -e
DIR="$(cd "$(dirname "$0")/.." && pwd)"
chmod +x "$DIR/deploy/autoupdate.sh"
cat > /etc/cron.d/ozon-profit-update <<CRON
*/2 * * * * root flock -n /run/ozon-profit-update.lock $DIR/deploy/autoupdate.sh >> /var/log/ozon-profit-update.log 2>&1
CRON
chmod 644 /etc/cron.d/ozon-profit-update
echo "Автообновление включено. Журнал: tail -f /var/log/ozon-profit-update.log"
