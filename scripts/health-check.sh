#!/bin/bash
# Health check script for UK House Prices services

set -e

echo "=== UK House Prices Service Health Check ==="
echo "Date: $(date)"
echo

# Check if docker-compose services are running
echo "🐳 Docker Services Status:"
docker-compose ps
echo

# Check database connectivity and data freshness
echo "🗄️  Database Health:"
docker-compose exec -T postgres psql -U prices -d house_prices -c "
SELECT 
    'Data Freshness' as check_type,
    last_transaction_date,
    latest_ingestion,
    total_transactions,
    CASE 
        WHEN latest_ingestion > NOW() - INTERVAL '48 hours' THEN '✅ RECENT'
        WHEN latest_ingestion > NOW() - INTERVAL '7 days' THEN '⚠️  STALE' 
        ELSE '❌ OLD'
    END as status
FROM get_data_freshness();
"
echo

# Check disk usage
echo "💾 Disk Usage:"
docker-compose exec -T postgres du -sh /var/lib/postgresql/data
echo

# Check Grafana connectivity
echo "📊 Grafana Status:"
if curl -sf http://localhost:3000/api/health > /dev/null 2>&1; then
    echo "✅ Grafana is accessible"
else
    echo "❌ Grafana is not responding"
fi
echo

echo "=== Health Check Complete ==="