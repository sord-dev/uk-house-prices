# UK House Prices Data Pipeline

A self-hosted data pipeline that ingests HM Land Registry Price Paid Data into PostgreSQL and visualizes it through Grafana dashboards. Built for tracking residential property prices in target areas with automated monthly updates.

<img width="1597" height="952" alt="image" src="https://github.com/user-attachments/assets/c49e9b05-7daf-46f4-a1e5-901172046d1e" />

<img width="1598" height="955" alt="image" src="https://github.com/user-attachments/assets/0d8a10c6-9654-4187-baa2-93630a7ae3be" />

## Architecture

```
HM Land Registry API → Python Ingest Service → PostgreSQL 16 → Grafana Dashboard
                    ↓                            ↑
                ONS Postcode API         Materialized Views & Indexes
```

## Features

- **Complete historical data**: Ingest full Land Registry dataset (1995-present)
- **Automated updates**: Monthly delta processing with proper A/C/D record handling
- **Geographic filtering**: Target specific counties to keep dataset focused
- **Performance optimized**: Materialized views and strategic indexes for fast queries
- **Rich visualizations**: Time series analysis, geographic heatmaps, market statistics
- **Data quality**: Validates transactions, handles edge cases, structured logging

## Quick Start

### 1. Prerequisites

- Docker and Docker Compose
- 10GB+ free disk space (for filtered dataset)
- Reliable internet connection (initial download ~5GB)

### 2. Setup

```bash
# Clone/create project directory
cd /home/picxi/Desktop/projects/picxibox/uk-house-prices

# Copy environment template
cp .env.example .env

# Edit configuration
nano .env
```

### 3. Configure Environment

Edit `.env` with your settings:

```bash
# Database credentials
POSTGRES_PASSWORD=your_secure_password_here
GRAFANA_ADMIN_PASSWORD=admin_password_here

# Target areas (comma-separated, uppercase)
TARGET_COUNTIES=ESSEX,HERTFORDSHIRE,KENT,SURREY,CAMBRIDGESHIRE
```

### 4. Start Services

```bash
# Start database and Grafana
docker-compose up -d postgres grafana

# Wait for services to initialize (30-60 seconds)
docker-compose logs -f postgres

# Verify services are healthy
docker-compose ps
```

### 5. Initial Data Load

```bash
# Full historical dataset (takes 30-60 minutes)
docker-compose run --rm ingest python ingest.py --mode full

# OR start with a single year for testing
docker-compose run --rm ingest python ingest.py --mode yearly --year 2023
```

### 6. Access Dashboard

Open Grafana at `http://localhost:3000`
- Username: `admin`
- Password: (from your `.env` file)

The PostgreSQL datasource will be automatically configured.

## Usage

### Ingestion Modes

```bash
# Full dataset (1995-present) - run once for baseline
docker-compose run --rm ingest python ingest.py --mode full

# Specific year - useful for backfilling or testing
docker-compose run --rm ingest python ingest.py --mode yearly --year 2024

# Monthly updates - run on 21st of each month
docker-compose run --rm ingest python ingest.py --mode monthly

# Check what's available
docker-compose run --rm ingest python ingest.py --help
```

### Monitoring

```bash
# Check ingestion logs
docker-compose logs ingest

# Database status
docker-compose exec postgres psql -U prices -d house_prices -c "SELECT * FROM get_data_freshness();"

# Service health
docker-compose ps
```

### Maintenance

```bash
# Refresh materialized views manually
docker-compose exec postgres psql -U prices -d house_prices -c "SELECT refresh_monthly_stats();"

# Backup database
docker-compose exec postgres pg_dump -U prices house_prices > backup_$(date +%Y%m%d).sql

# Update container images
docker-compose pull
docker-compose up -d
```

## Configuration

### Geographic Targeting

Edit `TARGET_COUNTIES` in `.env` to change which areas are ingested:

```bash
# Example: Focus on London commuter belt
TARGET_COUNTIES=HERTFORDSHIRE,ESSEX,KENT,SURREY

# Example: Expand to wider South East
TARGET_COUNTIES=HERTFORDSHIRE,ESSEX,KENT,SURREY,BUCKINGHAMSHIRE,BERKSHIRE,OXFORDSHIRE
```

Re-run yearly ingests for new counties:
```bash
docker-compose run --rm ingest python ingest.py --mode yearly --year 2023
```

### Data Volume Sizing

Expected storage requirements (PostgreSQL + indexes):

| Scope | Transactions | Storage |
|-------|-------------|---------|
| Single county, 5 years | ~500K | ~200MB |
| 5 counties, full history | ~5M | ~2GB |
| England & Wales, full history | ~28M | ~15GB |

Mount point in docker-compose.yml should use fast storage (NVMe/SSD).

## Automation

### Cron Setup

Create monthly update cron job:

```bash
# Edit crontab
crontab -e

# Add entry to run on 21st of each month at 6 AM
0 6 21 * * cd /home/picxi/Desktop/projects/picxibox/uk-house-prices && docker-compose run --rm ingest python ingest.py --mode monthly
```

### Systemd Timer (Alternative)

```bash
# Copy timer files
sudo cp scripts/house-prices-update.* /etc/systemd/system/

# Enable and start
sudo systemctl enable house-prices-update.timer
sudo systemctl start house-prices-update.timer

# Check status
sudo systemctl list-timers house-prices*
```

## Dashboard Ideas

### 1. Price Trends Dashboard
- Median price over time by county (line chart)
- Property type breakdown (stacked area)
- New build vs existing (comparison)
- Filter controls: date range, property type, tenure

### 2. Geographic Dashboard  
- Postcode district heatmap (median prices)
- Transaction volume choropleth
- County-level aggregations
- Interactive drill-down capabilities

### 3. Market Analysis Dashboard
- Transaction volume trends (bar chart)
- Price distribution histograms
- Seasonal patterns analysis
- Market activity indicators

## Database Schema

### Key Tables

- `transactions` - Main price paid data with full Land Registry schema
- `postcodes` - ONS postcode directory for geocoding
- `market_transactions` - Clean view excluding non-standard sales
- `monthly_price_stats` - Pre-aggregated statistics for performance

### Example Queries

```sql
-- Median prices by county, last 12 months
SELECT 
    county,
    COUNT(*) as transactions,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price
FROM market_transactions 
WHERE date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY county
ORDER BY median_price DESC;

-- Monthly trend for specific area
SELECT 
    DATE_TRUNC('month', date) as month,
    property_type,
    COUNT(*) as count,
    median_price
FROM monthly_price_stats
WHERE county = 'HERTFORDSHIRE'
  AND month >= '2023-01-01'
ORDER BY month, property_type;
```

## Data Sources & Licensing

### HM Land Registry Price Paid Data
- **License**: Open Government Licence v3.0
- **Coverage**: England & Wales, 1995-present  
- **Updates**: Monthly on 20th working day
- **Lag**: 2 weeks to 2 months for registration
- **URL**: https://landregistry.data.gov.uk/

### ONS Postcode Directory
- **License**: Open Government Licence v3.0
- **Purpose**: Lat/long coordinates for mapping
- **Updates**: Quarterly
- **URL**: https://geoportal.statistics.gov.uk/

## Troubleshooting

### Common Issues

**Services won't start**
```bash
# Check logs for errors
docker-compose logs postgres grafana

# Verify .env file is present and valid
cat .env

# Check port conflicts
ss -tlpn | grep -E ":3000|:5432"
```

**Ingestion fails**
```bash
# Check database connectivity
docker-compose exec postgres psql -U prices -d house_prices -c "\dt"

# Verify Land Registry URLs are accessible
curl -I https://price-paid-data.publicdata.landregistry.gov.uk/pp-monthly-update-new-version.csv

# Check available disk space
df -h
```

**Grafana connection issues**
```bash
# Verify datasource configuration
docker-compose exec grafana cat /etc/grafana/provisioning/datasources/postgres.yml

# Test database connection
docker-compose exec postgres psql -U prices -h postgres -d house_prices -c "SELECT COUNT(*) FROM transactions;"
```

### Performance Tuning

**Slow queries**
```sql
-- Check index usage
SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read 
FROM pg_stat_user_indexes 
ORDER BY idx_scan DESC;

-- Analyze table statistics  
ANALYZE transactions;
ANALYZE monthly_price_stats;
```

**Large dataset optimization**
```sql
-- Partition large tables by date (for advanced users)
-- Consider archiving old data beyond analysis timeframe
-- Adjust PostgreSQL memory settings in docker-compose.yml
```

## Development

### Local Development

```bash
# Install Python dependencies
cd ingest
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run tests
python -m pytest tests/

# Lint code
python -m flake8 ingest.py
```

### Contributing

1. Follow existing code structure and patterns
2. Add tests for new functionality  
3. Update documentation for any schema changes
4. Test with representative data samples

## License

This project is licensed under MIT License. 

Land Registry and ONS data are provided under Open Government Licence v3.0.

## Support

For issues with this pipeline:
- Check logs: `docker-compose logs`
- Review troubleshooting section above
- Check database connectivity and disk space

For data issues:
- Land Registry: https://landregistry.data.gov.uk/
- ONS Postcode: https://geoportal.statistics.gov.uk/
