# Sales Analytics Dashboard using Cloud Storage
**Project-Based Learning — Big Data Analysis & Cloud Computing**

A working, end-to-end big-data system: millions of sales records live in **cloud object storage**
(an S3-style data lake), are cleaned by a **parallel ETL pipeline** into partitioned **Parquet**,
analysed with a **columnar SQL engine (DuckDB)**, served through a **REST API (Flask)** and
explored in an interactive **dashboard**.

| | |
|---|---|
| Data volume | ~2 million order lines (270 MB raw CSV), configurable up to 10M+ |
| Storage | S3-compatible object storage — local emulation, MinIO, AWS S3 or Google Cloud Storage |
| Big-data techniques | Data lake zones, Parquet + zstd, Hive partitioning, partition pruning, map/reduce-style parallel ETL |
| Analytics | KPIs & YoY, trends, pivots, heat-map, **RFM segmentation**, **cohort retention**, **market-basket (lift)**, **forecast**, **anomaly detection** |
| Query speed | 20–300 ms per query over 2M rows (shown live in the header) |

## Run it (3 commands)

```bash
pip install -r requirements.txt
python -m etl.generate_data --rows 2000000     # 1. creates raw CSV files in cloud storage   (~20 s)
python -m etl.pipeline                          # 2. clean + convert to partitioned Parquet    (~10 s)
python app.py                                   # 3. open http://localhost:5000
```
(or simply `./scripts/run_all.sh`)

## Use a real cloud (optional, for extra marks)

The same code runs on any S3-compatible service — only environment variables change
(see `.env.example`).

```bash
# MinIO + app in Docker (a private cloud on your laptop)
docker compose up --build
docker compose run --rm app python -m etl.generate_data --rows 2000000
docker compose run --rm app python -m etl.pipeline

# AWS S3
export STORAGE_BACKEND=s3 BUCKET_NAME=my-unique-bucket AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_DEFAULT_REGION=ap-south-1
python -m etl.generate_data && python -m etl.pipeline && python app.py
```
Open the **Cloud Data Lake** tab to see the bucket contents live.

## Demo script for your presentation (8–10 minutes)

1. **Cloud Data Lake tab** – show the architecture, the zones (`raw/`, `curated/`, `quarantine/`), the compression and the ETL data-quality report.
2. **Overview tab** – change the date range / region / category; point at the *⚡ Query* pill: DuckDB answers in milliseconds.
3. **Customers tab** – Champions are ~20 % of customers but ~55 % of revenue; basket rules → bundle ideas.
4. **Forecast tab** – 6-month forecast with confidence band; the detector finds festival spikes and outage days.
5. **Live ingestion** (Cloud tab) – download the sample CSV, upload it, watch it flow *bucket → ETL → dashboard*. Upload the same file again → blocked by the checksum (idempotency).

## Project layout

```
config.py                 all settings (env-var driven)
storage/object_store.py   cloud-storage abstraction: LocalObjectStore | S3ObjectStore
etl/generate_data.py      synthetic Indian retail data (growth, Diwali, outages, dirty rows)
etl/pipeline.py           raw -> clean -> Parquet (parallel map tasks, reduce = run report)
analytics/engine.py       DuckDB SQL analytics (parameterised, partition-pruned)
analytics/forecasting.py  seasonal-trend forecast + robust anomaly detection (NumPy)
app.py                    Flask REST API, LRU cache, upload/ingest endpoint, CSV export
frontend/                 single-page dashboard (vanilla JS + Chart.js, works offline)
tests/                    pytest unit tests
docs/                     ARCHITECTURE.md, VIVA_QA.md
```

## API cheat-sheet
`/api/kpis` `/api/trend?grain=month` `/api/breakdown/<category|region|state|city|channel|payment_method>`
`/api/top-products` `/api/heatmap` `/api/rfm` `/api/cohorts` `/api/basket` `/api/forecast` `/api/anomalies`
`/api/lake` `POST /api/ingest` `/api/export/<monthly|rfm|…>.csv` — all accept `from`, `to`, `region`, `category`, `channel`.

## Run the tests
```bash
pytest -q
```
