"""Big-data ETL pipeline:  raw CSV (bronze)  ->  clean, typed, partitioned Parquet (silver).

Design (talking points for viva):
  * MAP     - every raw object is processed independently in its own worker process
              (data parallelism, the same idea as Hadoop/Spark map tasks)
  * REDUCE  - the per-file statistics are merged into one run report
  * Columnar Parquet + zstd compression + Hive partitioning (year=/month=) -> fast, cheap scans
  * Bad rows are never silently dropped: they go to quarantine/ with a rejection reason
  * Idempotent - re-running overwrites the same part files, never duplicates data

Usage:  python -m etl.pipeline            (full rebuild of curated/)
"""
from __future__ import annotations

import argparse
import re
import shutil
import tempfile
import time
import warnings
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from multiprocessing import get_context
from pathlib import Path

import numpy as np
import pandas as pd

import config
from etl.schema import CANONICAL, RAW_COLUMNS
from storage import get_store


# ---------------------------------------------------------------------------
def _canon(series: pd.Series, allowed: list[str]) -> pd.Series:
    mapping = {a.lower(): a for a in allowed}
    return series.astype("string").str.strip().str.lower().map(mapping)


def clean_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, Counter]:
    """Validate + standardise + enrich. Returns (clean, rejected, reason_counts)."""
    missing = [c for c in RAW_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    df = df[RAW_COLUMNS].copy()

    # 1. standardise text (trim, canonical spelling)
    for col, allowed in CANONICAL.items():
        df[col] = _canon(df[col], allowed)
    for col in ["order_id", "customer_id", "product_id", "product_name", "category", "state", "city"]:
        df[col] = df[col].astype("string").str.strip()
        df.loc[df[col] == "", col] = pd.NA

    # 2. parse types (bad values become NaN/NaT instead of crashing)
    s = df["order_date"].astype("string").str.strip()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        d = pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")
        retry = d.isna() & s.notna()
        if retry.any():
            d[retry] = pd.to_datetime(s[retry], errors="coerce", dayfirst=True)
    df["order_date"] = d
    for col in ["line_no", "quantity", "unit_price", "unit_cost", "discount_pct"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["discount_pct"] = df["discount_pct"].fillna(0.0)          # business rule: no discount recorded = 0

    # 3. rules, evaluated in priority order -> one reason per rejected row
    reason = pd.Series(pd.NA, index=df.index, dtype="string")
    rules = [
        ("duplicate_row", df.duplicated(subset=["order_id", "line_no"], keep="first")),
        ("missing_customer", df["customer_id"].isna()),
        ("missing_order_or_product", df["order_id"].isna() | df["product_id"].isna()),
        ("invalid_date", df["order_date"].isna()),
        ("invalid_quantity", ~(df["quantity"] >= 1) | (df["quantity"] > 1000)),
        ("invalid_price", ~(df["unit_price"] > 0) | (df["unit_price"] > 500_000) | ~(df["unit_cost"] >= 0)),
        ("invalid_discount", (df["discount_pct"] < 0) | (df["discount_pct"] > 0.9)),
        ("unknown_dimension", df["region"].isna() | df["channel"].isna() | df["payment_method"].isna()
         | df["category"].isna() | df["state"].isna()),
    ]
    for name, cond in rules:
        reason = reason.mask(reason.isna() & cond.fillna(True), name)

    bad = reason.notna()
    rejected = df[bad].copy()
    rejected["reject_reason"] = reason[bad]
    counts = Counter(reason[bad].tolist())
    out = df[~bad].copy()

    # 4. enrich with business metrics (the "T" in ETL)
    out["quantity"] = out["quantity"].astype("int32")
    out["line_no"] = out["line_no"].astype("int16")
    out["gross_amount"] = (out["quantity"] * out["unit_price"]).round(2)
    out["discount_amount"] = (out["gross_amount"] * out["discount_pct"]).round(2)
    out["revenue"] = (out["gross_amount"] - out["discount_amount"]).round(2)
    out["cost"] = (out["quantity"] * out["unit_cost"]).round(2)
    out["profit"] = (out["revenue"] - out["cost"]).round(2)
    out["dow"] = out["order_date"].dt.dayofweek.astype("int8")            # Monday = 0
    out["is_weekend"] = out["dow"] >= 5
    out["quarter"] = out["order_date"].dt.quarter.astype("int8")
    out["_year"] = out["order_date"].dt.year
    out["_month"] = out["order_date"].dt.month
    out["order_date"] = out["order_date"].dt.date                       # -> Parquet DATE
    return out, rejected, counts


def _file_id(key: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "-", Path(key).stem)[:80]


def process_raw_object(key: str) -> dict:
    """MAP task: one raw object in, partitioned Parquet files + a stats dict out."""
    t0 = time.time()
    store = get_store()
    tmp = Path(tempfile.mkdtemp(prefix="etl_"))
    try:
        local = tmp / "raw.csv"
        store.download_file(key, local)
        raw_bytes = local.stat().st_size
        df = pd.read_csv(local, dtype=str)
        rows_in = len(df)
        clean, rejected, counts = clean_dataframe(df)

        parquet_bytes, parts = 0, []
        fid = _file_id(key)
        for (y, m), g in clean.groupby(["_year", "_month"]):
            g = g.drop(columns=["_year", "_month"]).sort_values(["order_date", "order_id", "line_no"])
            f = tmp / f"part-{fid}-{y}-{m}.parquet"
            g.to_parquet(f, engine="pyarrow", compression="zstd", index=False)
            k = f"{config.ZONE_CURATED}year={y}/month={m:02d}/part-{fid}.parquet"
            store.put_file(f, k)
            parquet_bytes += f.stat().st_size
            parts.append(k)
        if len(rejected):
            f = tmp / "rejected.csv"
            rejected.drop(columns=["_year", "_month"], errors="ignore").to_csv(f, index=False)
            store.put_file(f, f"{config.ZONE_QUARANTINE}{fid}.csv")
        return {"key": key, "rows_in": rows_in, "rows_out": len(clean), "rows_rejected": len(rejected),
                "reasons": dict(counts), "raw_bytes": raw_bytes, "parquet_bytes": parquet_bytes,
                "partitions": parts, "seconds": round(time.time() - t0, 2), "error": None}
    except Exception as e:  # keep the run alive, report the failure
        return {"key": key, "rows_in": 0, "rows_out": 0, "rows_rejected": 0, "reasons": {}, "raw_bytes": 0,
                "parquet_bytes": 0, "partitions": [], "seconds": round(time.time() - t0, 2), "error": str(e)}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def run_pipeline(keys: list[str] | None = None, full: bool = True, workers: int | None = None,
                 trigger: str = "manual", verbose: bool = False) -> dict:
    store = get_store()
    t0 = time.time()
    if keys is None:
        keys = [o["key"] for o in store.list_objects(config.ZONE_RAW) if o["key"].endswith(".csv")]
    if full:
        store.delete_prefix(config.ZONE_CURATED)
        store.delete_prefix(config.ZONE_QUARANTINE)
    if not keys:
        raise RuntimeError("No raw files found - run `python -m etl.generate_data` first.")

    workers = min(workers or config.ETL_WORKERS, len(keys))
    if workers <= 1:
        results = [process_raw_object(k) for k in keys]
    else:
        with ProcessPoolExecutor(max_workers=workers, mp_context=get_context("spawn")) as ex:
            results = []
            for i, r in enumerate(ex.map(process_raw_object, keys), 1):
                results.append(r)
                if verbose:
                    print(f"  [{i:>3}/{len(keys)}] {r['key']:<48} in={r['rows_in']:>8,} out={r['rows_out']:>8,} "
                          f"rejected={r['rows_rejected']:>5,}  {r['seconds']}s", flush=True)

    # REDUCE: merge per-file stats
    reasons = Counter()
    for r in results:
        reasons.update(r["reasons"])
    rows_in = sum(r["rows_in"] for r in results)
    rows_out = sum(r["rows_out"] for r in results)
    raw_b = sum(r["raw_bytes"] for r in results)
    pq_b = sum(r["parquet_bytes"] for r in results)
    now = datetime.now(timezone.utc)
    report = {
        "run_id": now.strftime("%Y%m%dT%H%M%S"), "finished_at": now.isoformat(), "trigger": trigger,
        "mode": "full-rebuild" if full else "incremental", "files": len(results), "workers": workers,
        "rows_in": rows_in, "rows_out": rows_out, "rows_rejected": rows_in - rows_out,
        "reject_rate": round((rows_in - rows_out) / rows_in, 5) if rows_in else 0,
        "reasons": dict(reasons.most_common()), "raw_bytes": raw_b, "parquet_bytes": pq_b,
        "compression_ratio": round(raw_b / pq_b, 2) if pq_b else None,
        "seconds": round(time.time() - t0, 2),
        "rows_per_second": int(rows_in / max(time.time() - t0, 1e-6)),
        "errors": [{"key": r["key"], "error": r["error"]} for r in results if r["error"]],
    }
    store.put_json(report, f"{config.ZONE_META}runs/{report['run_id']}.json")
    store.put_json(report, f"{config.ZONE_META}latest_run.json")
    return report


def main():
    ap = argparse.ArgumentParser(description="Run the raw -> curated ETL pipeline")
    ap.add_argument("--workers", type=int, default=config.ETL_WORKERS)
    a = ap.parse_args()
    print(f"Running ETL with {a.workers} parallel workers on {get_store().describe()['backend']} ...")
    rep = run_pipeline(workers=a.workers, verbose=True)
    print("\n=== PIPELINE REPORT ===")
    for k in ["files", "workers", "rows_in", "rows_out", "rows_rejected", "reject_rate", "compression_ratio",
              "seconds", "rows_per_second"]:
        print(f"{k:>18}: {rep[k]:,}" if isinstance(rep[k], int) else f"{k:>18}: {rep[k]}")
    print("  rejection reasons:", rep["reasons"])
    if rep["errors"]:
        print("  ERRORS:", rep["errors"])


if __name__ == "__main__":
    main()
