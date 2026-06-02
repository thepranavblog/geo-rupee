# geo-rupee

A real-time data pipeline that correlates geopolitical events with Indian Rupee (USD/INR) exchange rate movements. Ingests GDELT news events and live forex ticks, joins them in a Spark streaming job, transforms through a dbt star schema, and surfaces insights in a Streamlit dashboard.

## Architecture

```
GDELT API ──────┐
                ├──► Kafka ──► Spark Streaming ──► S3 (Parquet)
Yahoo Finance ──┘                                      │
                                                       ▼
                                              Airflow (every 15 min)
                                                       │
                                                    dbt run
                                                       │
                                                       ▼
                                                   DuckDB ──► Streamlit Dashboard
```

## Components

### Producers (`producers/`)
| File | Description |
|------|-------------|
| `gdelt_poller.py` | Polls [GDELT v2](http://data.gdeltproject.org/gdeltv2/lastupdate.txt) every 15 minutes, filters for 10 geopolitically relevant countries, and publishes events to the `geopolitical-events` Kafka topic. Events are partitioned by region (South Asia / East Asia / Middle East / Western). |
| `yahoo_finance_poller.py` | Polls `USDINR=X` via yfinance every 2 minutes and publishes close price ticks to the `market-data` Kafka topic. |

### Spark Streaming (`spark/`)
`streaming_job.py` runs a PySpark structured streaming job:
- Reads both Kafka topics concurrently with watermarking (events: 30 min, market: 10 min)
- Joins them over a 2-hour event window
- Computes USD/INR % change at **15 min**, **1 hr**, and **2 hr** after each geopolitical event
- Writes raw data to **bronze** S3 layers and enriched correlated data to the **silver** layer

### Airflow DAG (`airflow/dags/`)
`geopolitical_rupee_dag.py` runs every 15 minutes:
1. Waits for new silver Parquet data on S3
2. Runs dbt staging models (`stg_geopolitical_events`, `stg_market_movement`)
3. Runs intermediate model (`int_correlated_events`)
4. Runs mart models in parallel (`fact_correlation`, `dim_country`, `dim_event_type`)
5. Runs `dbt test`
6. Sends a failure alert if any task fails

### dbt Project (`dbt_project/`)
Three-layer star schema written to DuckDB:

```
staging/
  stg_geopolitical_events   -- cleaned GDELT events from S3 bronze
  stg_market_movement       -- cleaned forex ticks from S3 bronze
intermediate/
  int_correlated_events     -- joined events + rupee movement windows
marts/
  fact_correlation          -- central fact table (event + rupee changes)
  dim_country               -- country dimension with region grouping
  dim_event_type            -- CAMEO event category dimension
```

### Dashboard (`dashboard/`)
Streamlit app backed by DuckDB with auto-refresh:
- **KPI strip** — total events, avg rupee move at 1hr, biggest 2hr drop, most active country
- **CAMEO bar chart** — avg rupee change per event category
- **Country table + chart** — rupee impact broken down by country and region
- **Scatter plot** — Goldstein score vs rupee change at 1hr
- **Timeline** — daily avg USD/INR rate overlaid with min Goldstein score

## Relevant Countries
`IND`, `CHN`, `USA`, `RUS`, `UAE`, `SAU`, `PAK`, `GBR`, `JPN`, `DEU`

## S3 Bucket Layout
```
s3://geopolitical-rupee-pipeline/
  bronze/
    gdelt-raw/          -- raw GDELT events partitioned by event_date
    market-raw/         -- raw forex ticks partitioned by date
  silver/
    correlated/         -- enriched event+rupee data partitioned by date
  checkpoints/spark/    -- Spark streaming checkpoints
```

## Getting Started

### Prerequisites
- Docker & Docker Compose
- AWS credentials with S3 read/write access

### Environment Variables
Create a `.env` file in the project root:
```env
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=us-east-1
```

### Run
```bash
docker compose up --build
```

This starts:
| Service | Port | Description |
|---------|------|-------------|
| Kafka | 9092 | Message broker |
| Spark Master UI | 8080 | Spark web UI |
| Airflow Webserver | 8081 | DAG management |
| Streamlit Dashboard | 8501 | Live dashboard |

### Run Tests
```bash
pytest tests/
```

## Project Structure
```
geo-rupee/
├── producers/          # Kafka producers (GDELT + Yahoo Finance)
├── spark/              # PySpark structured streaming job
├── airflow/
│   └── dags/           # Airflow DAG
├── dbt_project/
│   └── models/
│       ├── staging/
│       ├── intermediate/
│       └── marts/
├── dashboard/          # Streamlit app + DuckDB queries
├── kafka/              # Topic setup script
├── tests/              # Unit + integration tests
└── docker-compose.yml
```
