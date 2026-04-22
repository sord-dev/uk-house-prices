# uk-house-prices — Project Spec

## what is this

a self-hosted data pipeline that ingests HM Land Registry Price Paid Data into postgres,
keeps it fresh with monthly updates, and exposes it through a grafana dashboard with
time series and geomap visualisation.

built for personal use — tracking residential sale prices in target areas outside the M25
over a 6-8 year deposit-saving timeline. the dataset compounds in value the longer it runs.

---

## goals

- ingest the full land registry price paid dataset (1995–present) as a one-time baseline
- apply monthly delta updates automatically
- filter to target geographies on ingest (not pull 5GB every month)
- visualise price trends by area, property type, tenure over time
- map view of median sale prices by postcode district
- enrich with ONS postcode → lat/long for geo queries
- future: join EPC data for price-per-sqm analysis

---

## data sources

### primary — HM Land Registry Price Paid Data

**licence:** Open Government Licence v3.0 — free for commercial and non-commercial use

**endpoints:**
```
# full history (one-time baseline, ~5GB)
https://price-paid-data.publicdata.landregistry.gov.uk/pp-complete.csv

# yearly files (115–230MB each)
https://price-paid-data.publicdata.landregistry.gov.uk/pp-{year}.csv

# monthly delta (updated on 20th working day of each month)
https://price-paid-data.publicdata.landregistry.gov.uk/pp-monthly-update-new-version.csv
```

**schema (no headers in file):**
```
transaction_id    -- unique, changes on re-registration (not a stable PK)
price             -- sale price on transfer deed
date              -- completion date
postcode
property_type     -- D=detached, S=semi, T=terraced, F=flat, O=other
new_build         -- Y/N
tenure            -- F=freehold, L=leasehold
paon              -- primary addressable object name (house number/name)
saon              -- secondary addressable object name (flat number etc)
street
locality
town
district
county
ppd_type          -- A=standard, B=additional (repossessions, buy-to-let etc)
record_status     -- A=add, C=change, D=delete
```

**important caveats:**
- last 2 months of data is always incomplete (registration lag of 2 weeks–2 months)
- locality field is inconsistent — use postcode as the primary geo key
- monthly delta includes A/C/D records — ingest must handle updates and deletes,
  not just inserts or prices will silently corrupt over time
- filter to `ppd_type = A` for clean market analysis
  (B records skew prices with non-standard transactions)

### enrichment — ONS Postcode Directory

**source:** https://geoportal.statistics.gov.uk/
**licence:** Open Government Licence v3.0

provides lat/long for every postcode — join key is postcode string.
required for grafana geomap panel. download once, update annually.

### future enrichment — EPC Dataset

**source:** https://epc.opendatacommunities.org/
**licence:** Open Government Licence v3.0

joins to PPD on postcode, adds total floor area and room count.
enables price-per-sqm analysis. match rate >90% for post-2011 transactions.
not in scope for v1.

---

## stack

```
land registry          ONS postcode
     |                      |
     v                      v
[python ingest]  ←→  [postgres 16]
                            |
                            v
                       [grafana]
                            |
                     dashboards + geomap
```

all containers on picxibox. grafana exposed via nginx proxy manager at
`https://prices.picxi.uk` (LAN only).

---

## services

### postgres

```yaml
image: postgres:16-alpine
container_name: house_prices_db
volumes:
  - ./data:/var/lib/postgresql/data
  - ./init:/docker-entrypoint-initdb.d
environment:
  POSTGRES_DB: house_prices
  POSTGRES_USER: prices
  POSTGRES_PASSWORD: <secret>
```

mount the M.2 SSD for the data volume — HDD is too slow for this workload.

### ingest (python)

runs as a one-shot container on cron. not a long-running service.

responsibilities:
- download CSV (full, yearly, or monthly delta based on mode)
- stream-parse (don't load 5GB into memory)
- filter rows by target geography before insert
- handle A/C/D record_status correctly
- upsert on transaction_id

key libraries: `httpx`, `psycopg3`, `csv` (stdlib)

### grafana

```yaml
image: grafana/grafana:latest
container_name: house_prices_grafana
volumes:
  - ./grafana:/var/lib/grafana
```

uses postgres datasource. no additional plugins required for geomap — it's built in.

---

## database schema

```sql
CREATE TABLE transactions (
    transaction_id   TEXT PRIMARY KEY,
    price            INTEGER NOT NULL,
    date             DATE NOT NULL,
    postcode         TEXT,
    property_type    CHAR(1),   -- D S T F O
    new_build        CHAR(1),   -- Y N
    tenure           CHAR(1),   -- F L
    paon             TEXT,
    saon             TEXT,
    street           TEXT,
    locality         TEXT,
    town             TEXT,
    district         TEXT,
    county           TEXT,
    ppd_type         CHAR(1),   -- A B
    record_status    CHAR(1),   -- A C D
    ingested_at      TIMESTAMPTZ DEFAULT now()
);

-- postcode lookup (joined at query time)
CREATE TABLE postcodes (
    postcode         TEXT PRIMARY KEY,
    lat              DOUBLE PRECISION,
    lng              DOUBLE PRECISION,
    district         TEXT,
    county           TEXT
);

-- indexes
CREATE INDEX idx_transactions_postcode ON transactions(postcode);
CREATE INDEX idx_transactions_date ON transactions(date);
CREATE INDEX idx_transactions_county ON transactions(county);
CREATE INDEX idx_transactions_type ON transactions(property_type);
```

**note on transaction_id:** land registry docs say the ID changes on re-registration.
use it as PK for upsert logic but don't treat it as a permanent stable reference.

---

## ingest logic

### modes

| mode | when | source |
|------|------|--------|
| `full` | one-time baseline | pp-complete.csv |
| `yearly` | backfill a specific year | pp-{year}.csv |
| `monthly` | cron job | pp-monthly-update-new-version.csv |

### record_status handling

```
A (add)    → INSERT ... ON CONFLICT (transaction_id) DO UPDATE
C (change) → same upsert — updated fields overwrite
D (delete) → DELETE WHERE transaction_id = ?
```

### geography filter

ingest accepts a list of target counties/districts.
rows not matching are discarded before insert — keeps the db lean.

target areas (update as deposit planning evolves):
```python
TARGET_COUNTIES = [
    "ESSEX",
    "HERTFORDSHIRE", 
    "KENT",
    "SURREY",
    "CAMBRIDGESHIRE",
]
```

can be extended or removed without re-ingesting — just re-run yearly mode for a new county.

---

## automation

systemd timer on picxibox, runs monthly on the 21st (day after land registry publishes):

```ini
[Timer]
OnCalendar=*-*-21 06:00:00
Persistent=true
```

or cron equivalent:
```
0 6 21 * * docker run --rm house_prices_ingest python ingest.py --mode monthly
```

---

## grafana dashboards — v1 scope

### dashboard 1 — price trends

- median sale price over time, by county (line chart)
- filter by: property type, tenure, new build flag
- filter by: date range
- table: recent transactions with all fields

### dashboard 2 — geomap

- postcode district level choropleth
- metric: median price, last 12 months
- tooltip: district name, median price, transaction count
- join: transactions → postcodes on postcode field

### dashboard 3 — market activity

- transaction volume by month (bar chart)
- new build vs existing split
- freehold vs leasehold split
- price distribution histogram by property type

---

## file structure

```
uk-house-prices/
  docker-compose.yml
  .env                    ← secrets, not committed
  ingest/
    Dockerfile
    ingest.py
    requirements.txt
  postgres/
    init/
      01_schema.sql
  grafana/
    provisioning/
      datasources/
        postgres.yml
  data/                   ← postgres data volume (on M.2)
  README.md
```

---

## out of scope for v1

- EPC enrichment (floor area / price per sqm)
- external access / public dashboard
- rightmove / zoopla scraping
- automated alerts (e.g. price drop in target area)
- price prediction / ML

---

## open questions

- which specific towns/districts to target within those counties?
- storage estimate: full filtered dataset (5 counties, 1995–present) probably 2–5GB in postgres
- grafana auth: admin only, LAN only — no public access
- should transaction_id be treated as truly unique or use a composite key (price + date + postcode + paon)?
  land registry docs suggest it can change — worth validating against real data
