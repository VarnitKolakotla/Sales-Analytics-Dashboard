# Viva Preparation — likely questions & answers

## Big Data
**Q1. Why is this "big data"?**  It has the 4 Vs: *Volume* (2M+ lines, scalable to 10M+), *Variety* (CSV in, JSON manifests, Parquet out), *Velocity* (live upload → queryable in seconds), *Veracity* (≈1.2 % bad rows detected, quarantined and reported).

**Q2. Why Parquet instead of CSV?**  Columnar: a query reading 3 of 24 columns reads only those columns; same-type values compress well (3.2× smaller here); stores schema/types; min/max statistics let engines skip data.

**Q3. What is partitioning / partition pruning?**  Files are grouped in `year=2025/month=03/`. A query for 2025 never opens other years. Less I/O → faster and cheaper on cloud storage.

**Q4. Where is MapReduce in your project?**  `process_raw_object` = map (independent per file, parallel). Merging counters in `run_pipeline` = reduce. Same idea as Hadoop/Spark, on one machine.

**Q5. Why DuckDB and not Hadoop/Spark?**  Right tool for the size: no cluster to run, vectorised columnar execution gives millisecond answers on millions of rows. Because data is stored as Parquet in a bucket, moving to Spark/Athena later needs no data conversion.

**Q6. What is a data lake / medallion architecture?**  Raw data kept as-is (bronze), cleaned trusted data (silver), business aggregates (gold). Here: `raw/` and `curated/` (+ `quarantine/`). Gold = the API's aggregate queries.

**Q7. How do you ensure data quality?**  Explicit rules in `clean_dataframe`, each rejected row gets one reason, rejects are stored (audit trail), the dashboard shows the reject rate by reason, unit tests check the rules.

## Cloud Computing
**Q8. Where is cloud computing here?**  Storage is object storage accessed by an S3 API. The same code runs against local emulation, MinIO (Docker) or AWS S3 by changing environment variables. The API is stateless and containerised (Dockerfile), so compute scales horizontally independent of storage.

**Q9. IaaS / PaaS / SaaS?**  S3/MinIO bucket and a VM or container host = IaaS. Managed Athena/BigQuery/Cloud Run would be PaaS. The dashboard itself is SaaS from the end-user's view.

**Q10. Why object storage rather than a database or file server?**  Virtually unlimited, very cheap, durable (11 nines on S3), accessed over HTTP, decoupled from compute, and every big-data engine reads from it.

**Q11. Is it elastic?**  Storage grows without limit; run more API containers behind a load balancer for more users; ETL workers scale by raising `ETL_WORKERS` or running as serverless jobs.

**Q12. Security?**  Parameterised SQL, whitelist validation, upload type/size limit, path-traversal guard in the store, credentials only via environment variables (never in code). To add: HTTPS, authentication, bucket policies, encryption at rest.

## Analytics
**Q13. Explain RFM.**  Recency (days since last order), Frequency (orders), Monetary (spend). Each scored 1–5 with `NTILE(5)`; combinations map to segments such as Champions or At Risk, each with a marketing action.

**Q14. What does *lift* mean in market-basket analysis?**  Lift = P(A and B) / (P(A)·P(B)). 1 = independent; 40 = bought together 40× more than chance → bundle them.

**Q15. How does the forecast work and how accurate is it?**  Log-linear growth trend × monthly seasonal index, fitted by back-fitting; interval = ±1.96 × residual std. In-sample MAPE ≈ 4 % on this data. It is explainable; limitation: cannot foresee one-off events.

**Q16. How are anomalies found?**  Remove weekday effect, compare each day with the rolling 29-day median, scale by MAD (robust to outliers), flag |z| > 3.5. It finds the injected outages and the sale/festival spikes.

**Q17. What would you improve?**  Spark/Glue for ETL at TB scale, Redis cache, authentication, Prophet/ARIMA/LightGBM comparison, streaming ingestion (Kafka/Kinesis), data catalogue, CI/CD, Terraform for the cloud resources.

## Demo troubleshooting
* *Blank dashboard* → run the generate + pipeline commands first (banner explains it).
* *Port busy* → `PORT=5055 python app.py`.
* *Start from a clean state* → delete `cloud_storage/` and `.cache/`, then generate again.
