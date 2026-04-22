-- UK House Prices Database Schema
-- HM Land Registry Price Paid Data + ONS Postcode Directory

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Main transactions table (HM Land Registry Price Paid Data)
CREATE TABLE transactions (
    transaction_id   TEXT PRIMARY KEY,
    price            INTEGER NOT NULL,
    date             DATE NOT NULL,
    postcode         TEXT,
    property_type    CHAR(1),   -- D=detached, S=semi, T=terraced, F=flat, O=other
    new_build        CHAR(1),   -- Y=yes, N=no
    tenure           CHAR(1),   -- F=freehold, L=leasehold
    paon             TEXT,      -- primary addressable object name (house number/name)
    saon             TEXT,      -- secondary addressable object name (flat number etc)
    street           TEXT,
    locality         TEXT,
    town             TEXT,
    district         TEXT,
    county           TEXT,
    ppd_type         CHAR(1),   -- A=standard, B=additional (repossessions, buy-to-let etc)
    record_status    CHAR(1),   -- A=add, C=change, D=delete
    ingested_at      TIMESTAMPTZ DEFAULT now()
);

-- ONS Postcode Directory for geocoding
CREATE TABLE postcodes (
    postcode         TEXT PRIMARY KEY,
    lat              DOUBLE PRECISION,
    lng              DOUBLE PRECISION,
    district         TEXT,
    county           TEXT,
    region           TEXT,
    country          TEXT,
    ingested_at      TIMESTAMPTZ DEFAULT now()
);

-- Performance indexes for common queries
CREATE INDEX idx_transactions_postcode ON transactions(postcode);
CREATE INDEX idx_transactions_date ON transactions(date);
CREATE INDEX idx_transactions_county ON transactions(county);
CREATE INDEX idx_transactions_district ON transactions(district);
CREATE INDEX idx_transactions_property_type ON transactions(property_type);
CREATE INDEX idx_transactions_ppd_type ON transactions(ppd_type);
CREATE INDEX idx_transactions_tenure ON transactions(tenure);
CREATE INDEX idx_transactions_new_build ON transactions(new_build);

-- Composite indexes for common filter combinations
CREATE INDEX idx_transactions_county_date ON transactions(county, date);
CREATE INDEX idx_transactions_county_type ON transactions(county, property_type);
CREATE INDEX idx_transactions_date_type ON transactions(date, property_type);

-- Postcode indexes
CREATE INDEX idx_postcodes_district ON postcodes(district);
CREATE INDEX idx_postcodes_county ON postcodes(county);
CREATE INDEX idx_postcodes_location ON postcodes(lat, lng);

-- Create view for clean market analysis (excludes non-standard transactions)
CREATE VIEW market_transactions AS
SELECT 
    transaction_id,
    price,
    date,
    postcode,
    property_type,
    new_build,
    tenure,
    paon,
    saon,
    street,
    locality,
    town,
    district,
    county,
    ingested_at
FROM transactions 
WHERE ppd_type = 'A'  -- Standard price paid transactions only
  AND record_status = 'A'  -- Active records only
  AND price > 0;  -- Valid prices only

-- Create materialized view for performance - monthly aggregations
CREATE MATERIALIZED VIEW monthly_price_stats AS
SELECT 
    DATE_TRUNC('month', date) as month,
    county,
    district,
    property_type,
    tenure,
    new_build,
    COUNT(*) as transaction_count,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price,
    AVG(price) as mean_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    STDDEV(price) as price_stddev
FROM market_transactions
GROUP BY 
    DATE_TRUNC('month', date),
    county,
    district,
    property_type,
    tenure,
    new_build;

-- Index on materialized view
CREATE INDEX idx_monthly_stats_month_county ON monthly_price_stats(month, county);
CREATE INDEX idx_monthly_stats_county_type ON monthly_price_stats(county, property_type);

-- Refresh function for materialized view
CREATE OR REPLACE FUNCTION refresh_monthly_stats()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW monthly_price_stats;
END;
$$ LANGUAGE plpgsql;

-- Function to get latest data timestamp
CREATE OR REPLACE FUNCTION get_data_freshness()
RETURNS TABLE(
    last_transaction_date DATE,
    latest_ingestion TIMESTAMPTZ,
    total_transactions BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        MAX(t.date) as last_transaction_date,
        MAX(t.ingested_at) as latest_ingestion,
        COUNT(*)::BIGINT as total_transactions
    FROM transactions t
    WHERE t.ppd_type = 'A' 
      AND t.record_status = 'A';
END;
$$ LANGUAGE plpgsql;

-- Comments for documentation
COMMENT ON TABLE transactions IS 'HM Land Registry Price Paid Data - all property sales in England and Wales';
COMMENT ON TABLE postcodes IS 'ONS Postcode Directory - geographic coordinates for UK postcodes';
COMMENT ON VIEW market_transactions IS 'Clean view of standard market transactions, excluding non-standard sales';
COMMENT ON MATERIALIZED VIEW monthly_price_stats IS 'Pre-aggregated monthly statistics for dashboard performance';

-- Grant permissions (adjust as needed for your setup)
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_user;
-- GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO grafana_user;