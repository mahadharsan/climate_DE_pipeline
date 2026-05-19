# climate-de-pipeline

> Production-grade ELT pipeline ingesting Los Angeles weather data from Open-Meteo API through GCS → BigQuery → dbt medallion architecture, orchestrated with Dagster.

![Dagster Asset Graph](docs/dagster_asset_graph.png)

---

## Project Overview

A production-grade data engineering pipeline that ingests two years of historical weather data for Los Angeles from the Open-Meteo API, stores raw data in Google Cloud Storage, loads and transforms it through a medallion architecture in BigQuery using dbt, and orchestrates the full pipeline with Dagster.

The gold layer produces 15 weather features including lag signals, rolling averages, and anomaly flags — designed as the data foundation for a weather-driven demand forecasting model.

**This is Part 1 of a two-part project:**
- Part 1 (this repo): Data engineering pipeline — raw weather data to analytics-ready features
- Part 2 (in progress): Demand forecasting model — weather signals joined with Walmart M5 sales data to predict demand variability

---

## Architecture

```
Open-Meteo API (free, no auth)
        ↓
Python ingestion + PyArrow schema validation
        ↓
GCS Bucket — Bronze Layer (raw Parquet files)
        ↓
BigQuery — Raw Table (weather_raw dataset)
        ↓
dbt — Silver Layer (stg_weather: cleaned and cast)
        ↓
dbt — Gold Layer (mart_weather_daily: 15 engineered features)
        ↓
Dagster — Orchestration (asset graph, daily schedule, one-click run)
```

### Medallion Architecture

| Layer | Dataset | Table | Description |
|-------|---------|-------|-------------|
| Bronze | GCS Bucket | `bronze/weather/los_angeles/*.parquet` | Raw Parquet files exactly as received from API |
| Raw | `weather_raw` | `weather_raw_los_angeles` | Loaded from GCS, no transformation |
| Silver | `weather_staging` | `stg_weather` | Cleaned types, date cast, float precision fixed |
| Gold | `weather_marts` | `mart_weather_daily` | 15 analytics-ready features for forecasting |

---

## Tech Stack

| Tool | Purpose | Why |
|------|---------|-----|
| Python | Ingestion and loading scripts | Core language |
| Open-Meteo API | Weather data source | Free, no API key, historical data back to 1940 |
| PyArrow | Schema validation and Parquet conversion | Explicit schema enforcement, production-grade columnar format |
| Google Cloud Storage | Bronze layer file storage | Raw data safety net, replay capability |
| BigQuery | Cloud data warehouse | SQL-native, scales to petabytes, dbt native integration |
| dbt | SQL transformations | Modular, testable, documented transformation logic |
| Dagster | Orchestration | Asset-based thinking, native dbt integration, visual lineage |
| SNAPPY compression | Parquet compression | Industry standard, fast read/write for analytical workloads |

---

## Pipeline Flow

**Step 1 — Ingest (`ingestion/ingest.py`)**
- Calls Open-Meteo archive API for Los Angeles (lat: 34.0522, lon: -118.2437)
- Date range: 2023-01-01 to 2025-01-01 (732 days)
- Variables: temperature max/min, precipitation, wind speed, weather code
- Validates every row against explicit PyArrow schema
- Non-nullable columns (date, city, temperature) fail loudly if null
- Converts to Parquet with SNAPPY compression
- Uploads to GCS at `bronze/weather/los_angeles/`

**Step 2 — Load (`loading/load_to_bq.py`)**
- Reads Parquet file from GCS bronze layer
- Loads into BigQuery `weather_raw.weather_raw_los_angeles`
- Explicit schema definition — no autodetect
- WRITE_TRUNCATE disposition ensures idempotency

**Step 3 — Silver (`dbt/models/staging/stg_weather.sql`)**
- Casts date string to DATE type
- Rounds float32 values to 1 decimal place
- Filters null dates and temperatures
- Output: 732 clean rows in `weather_staging.stg_weather`

**Step 4 — Gold (`dbt/models/marts/mart_weather_daily.sql`)**
- Calculates 15 weather features for demand forecasting
- Output: 732 rows in `weather_marts.mart_weather_daily`

**Step 5 — Orchestration (`orchestration/definitions.py`)**
- Dagster registers all four assets
- dbt models automatically loaded from manifest.json
- Full pipeline runs with one click or on daily 6am schedule

---

## Gold Layer — Data Dictionary

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Date of observation |
| city | STRING | Los Angeles |
| temperature_max | FLOAT | Max daily temperature °C |
| temperature_min | FLOAT | Min daily temperature °C |
| precipitation_sum | FLOAT | Total daily precipitation mm |
| wind_speed_max | FLOAT | Max daily wind speed km/h |
| weather_code | INT | WMO weather condition code |
| weather_description | STRING | Human readable weather label |
| temperature_range | FLOAT | Max minus min — daily volatility |
| rolling_avg_temp_7d | FLOAT | 7-day rolling average temperature |
| temp_lag_3 | FLOAT | Temperature 3 days ago |
| temp_lag_7 | FLOAT | Temperature 7 days ago |
| anomaly_flag | INT | 1 if temp deviates >2 std devs from monthly average |
| season | STRING | Winter / Spring / Summer / Fall |
| is_extreme_heat | INT | 1 if temperature_max > 35°C |
| is_heavy_rain | INT | 1 if precipitation_sum > 10mm |

---

## Production Engineering Principles

**Idempotency** — WRITE_TRUNCATE on BigQuery load ensures running the pipeline twice produces the same result as running it once. No duplicate rows.

**Fail fast** — PyArrow schema validation catches null values in critical columns before data reaches GCS or BigQuery. Pipeline stops immediately with a clear error message.

**Schema enforcement** — Explicit schema definition on both PyArrow and BigQuery load. Auto-detection is never used. Source changes fail loudly.

**Separation of concerns** — Each script has one job. ingest.py only fetches and uploads. load_to_bq.py only loads. dbt models only transform. Dagster only orchestrates.

**Raw layer preservation** — Raw Parquet files in GCS are never modified. If transformations break, the pipeline can be replayed from the bronze layer without re-calling the API.

**Modular configuration** — All environment variables in settings.yaml. No hardcoded values in code. Switching environments requires changing one file.

---

## Repository Structure

```
climate-de-pipeline/
  ├── config/
  │     ├── settings.yaml          ← environment configuration
  │     └── gcp_credentials.json  ← never committed (in .gitignore)
  ├── ingestion/
  │     ├── schema.py              ← PyArrow schema definition
  │     └── ingest.py              ← API fetch + GCS upload
  ├── loading/
  │     └── load_to_bq.py          ← GCS to BigQuery loader
  ├── dbt/
  │     ├── models/
  │     │     ├── staging/
  │     │     │     ├── stg_weather.sql
  │     │     │     └── schema.yml
  │     │     └── marts/
  │     │           ├── mart_weather_daily.sql
  │     │           └── schema.yml
  │     ├── macros/
  │     │     └── generate_schema_name.sql
  │     └── dbt_project.yml
  ├── orchestration/
  │     ├── __init__.py
  │     ├── assets.py              ← Dagster asset definitions
  │     └── definitions.py         ← Dagster entry point
  ├── docs/
  │     └── dagster_asset_graph.png
  ├── .gitignore
  └── requirements.txt
```

---

## How to Run

**Prerequisites:** Python 3.13, GCP account with BigQuery and GCS enabled

**1. Clone the repo**
```bash
git clone https://github.com/mahadharsan/climate-de-pipeline.git
cd climate-de-pipeline
```

**2. Create virtual environment**
```bash
python -m venv venv
venv\Scripts\activate  # Windows
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

**4. Configure GCP credentials**
- Create a GCP project and service account with Storage Object Admin, BigQuery Data Editor, BigQuery Job User roles
- Download JSON key to `config/gcp_credentials.json`
- Update `config/settings.yaml` with your project ID and bucket name

**5. Run individually**
```bash
python ingestion/ingest.py
python loading/load_to_bq.py
cd dbt && dbt run
```

**6. Or run with Dagster**
```bash
cd dbt && dbt parse
cd ..
dagster dev -f orchestration/definitions.py
```
Open `http://localhost:3000` → Click Materialize all

---

## What's Next — Part 2

The gold layer is designed as the input for a weather-driven demand forecasting model.

Part 2 will:
- Ingest Walmart M5 sales data for California stores
- Join with Los Angeles weather data on date
- Build Prophet vs XGBoost demand forecasting models
- Show forecast accuracy improvement from adding weather signals
- Generate inventory buffer recommendations from forecast output

---

## Author

**Mahadharsan Ravichandran**
- LinkedIn: [linkedin.com/in/mahadharsan](https://linkedin.com/in/mahadharsan)
- GitHub: [github.com/mahadharsan](https://github.com/mahadharsan)
- Portfolio: [mahadharsan.netlify.app](https://mahadharsan.netlify.app)