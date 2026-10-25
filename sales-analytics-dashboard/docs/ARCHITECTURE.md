# Architecture & Design Decisions

```
 CSV / POS / App exports
          |
          v
 +-----------------------------  CLOUD OBJECT STORAGE (bucket) ----------------------------+
 |  raw/ (bronze)  --->  ETL (parallel workers)  --->  curated/ (silver)  + quarantine/    |
 |  CSV as received       validate · clean · enrich     Parquet, year=/month= partitions    |
 |  _meta/ : run manifests, ingestion checksums                                             |
 +-------------------------------------------|----------------------------------------------+
                                             v
                          DuckDB (columnar, vectorised, multi-threaded SQL)
                                             |
                       Flask REST API  (parameterised SQL · LRU cache · timing)
                                             |
                          Dashboard (filters · charts · heat-maps · live ingestion)
```

## 1. Cloud storage layer (`storage/object_store.py`)
* One interface (`put_file`, `get_bytes`, `list_objects`, `delete_prefix`, `materialize` …) with two back-ends.
* `LocalObjectStore` emulates a bucket on disk (atomic publish like an S3 PUT, path-traversal guard).
* `S3ObjectStore` uses boto3 → works with **AWS S3, MinIO, Google Cloud Storage (interop), Wasabi**. Multipart uploads, retries, paginated listing, incremental local cache of the curated zone.
* **Why:** decouples storage from compute (a core cloud principle) — the API containers are stateless and can be scaled or replaced without moving data.

## 2. Data lake zones
| Zone | Role | Format |
|---|---|---|
| `raw/` | Bronze – immutable landing area, replayable | CSV |
| `curated/` | Silver – trusted, typed, partitioned | Parquet (zstd), `year=/month=` |
| `quarantine/` | Rejected rows + reason, for audit and fixing the source | CSV |
| `_meta/` | Run reports, ingestion checksums | JSON |

## 3. ETL pipeline (`etl/pipeline.py`)
* **Map:** `process_raw_object()` handles one raw file in its own process (spawn pool) – data parallelism like Hadoop/Spark map tasks.
* **Reduce:** `run_pipeline()` merges per-file counters into one report (rows, rejects by reason, compression, throughput).
* **Rules:** duplicates, missing customer/product, invalid date/quantity/price/discount, unknown dimension values. Text is canonicalised (`" south "` → `South`).
* **Enrichment:** gross, discount, revenue, cost, profit, weekday, quarter.
* **Idempotent:** deterministic part-file names → re-runs overwrite, never duplicate. Dashboard uploads are also guarded by a SHA-256 checksum registry.

## 4. Analytics engine (`analytics/engine.py`)
* DuckDB reads Parquet in place: only the needed columns are scanned (columnar), year folders are pruned (`year BETWEEN`), all cores are used.
* Techniques: GROUP BY, window functions (`SUM() OVER`, `NTILE`), self-joins (basket), CTEs, date arithmetic.
* **RFM:** quintile scores → 9 segments with actions.
* **Cohorts:** first-purchase month × months-since, retention %.
* **Market basket:** support, confidence, lift for product pairs.
* **Forecast:** log-linear trend × seasonal index fitted by back-fitting (alternating trend / seasonality estimation), 95 % interval from residual spread.
* **Anomalies:** weekday-adjusted daily sales, rolling median + MAD robust z-score (|z| > 3.5).
* Filters are **bound parameters**; dimensions are whitelisted → no SQL injection.

## 5. API (`app.py`)
* Decorator adds: LRU response cache, query timing (`meta.query_ms`), uniform error JSON.
* Upload endpoint: type + size validation, store → ETL → refresh engine → clear cache.

## 6. Scaling story (for "what if the data is 1000× bigger?")
1. Keep the Parquet files; replace DuckDB with **Spark / AWS Athena / BigQuery** reading the same bucket.
2. Run ETL as **Spark / AWS Glue** jobs triggered by S3 events (new object → job).
3. Put the API behind a load balancer (containers on ECS / Cloud Run) with Redis instead of the in-process cache.
4. Add Parquet compaction + partitioning by region for very large months.

## 7. Known limitations (be upfront in the viva)
* Data is **synthetic** (realistic patterns, but generated) — the pipeline accepts any CSV with the same columns.
* DuckDB is single-node; it handles hundreds of millions of rows, not petabytes.
* Forecast is a statistical model, not deep learning — chosen because it is explainable.
* No user authentication (add OAuth/JWT before real deployment).
