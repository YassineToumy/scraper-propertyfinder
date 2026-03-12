#!/bin/bash


echo "══════════════════════════════════════════════════"
echo "🇪🇬 PropertyFinder Service Starting"
echo "   Time: $(date -u)"
echo "══════════════════════════════════════════════════"

# Export env vars with proper shell quoting (URIs with @ ? & break plain sourcing)
env | while IFS='=' read -r k v; do
    [[ "$k" == "no_proxy" || "$k" == "_" ]] && continue
    printf 'export %s=%q\n' "$k" "$v"
done > /app/.env_runtime
printenv | grep -v -E "^(no_proxy|_)=" >> /etc/environment

cat > /etc/cron.d/propertyfinder <<'CRON'
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin

# Scraper — every 6h (Playwright browser, slow — needs multiple passes)
0 */6 * * * root /app/runner.sh scraper >> /app/logs/cron.log 2>&1

# Cleaner — every 6h, 30 min after scraper
30 */6 * * * root /app/runner.sh cleaner >> /app/logs/cron.log 2>&1

CRON

chmod 0644 /etc/cron.d/propertyfinder

echo "✅ Cron schedule installed:"
echo "   */6h      🕷️  Scraper (every 6 hours — Playwright)"
echo "   */6h+30m  🧹 Cleaner (after scraper)"
echo ""

echo "🔌 Testing MongoDB..."
python -c "
from pymongo import MongoClient
import os
c = MongoClient(os.environ['MONGODB_URI'], serverSelectionTimeoutMS=5000)
c.admin.command('ping')
print('  ✅ MongoDB OK')
c.close()
" || echo "  ❌ MongoDB connection failed"

echo ""
echo "🔄 Running initial jobs on startup..."

/app/runner.sh scraper  || echo "⚠️  Scraper startup failed — will retry via cron"
/app/runner.sh cleaner  || echo "⚠️  Cleaner startup failed — will retry via cron"

echo "══════════════════════════════════════════════════"
echo "✅ Startup complete — cron daemon starting"
echo "══════════════════════════════════════════════════"

exec cron -f
