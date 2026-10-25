"""Flask REST API + static dashboard server.

    GET  /                        dashboard (single-page app in /frontend)
    GET  /api/health              liveness + storage/engine info
    GET  /api/meta                filter options, date range, row counts
    GET  /api/kpis                headline KPIs + YoY growth + sparklines
    GET  /api/trend               revenue / profit time-series (day|week|month) + previous-year overlay
    GET  /api/breakdown/<dim>     category | region | state | city | channel | payment_method
    GET  /api/matrix              region x category revenue
    GET  /api/top-products
    GET  /api/heatmap             weekday x month revenue
    GET  /api/rfm                 RFM customer segmentation
    GET  /api/cohorts             monthly cohort retention
    GET  /api/basket              market-basket analysis (support / confidence / lift)
    GET  /api/forecast            6-month revenue forecast
    GET  /api/anomalies           outlier days
    GET  /api/lake                cloud-storage zones, objects, ETL run reports
    POST /api/ingest              upload a CSV -> cloud storage -> ETL -> dashboard refresh
    GET  /api/sample.csv          download a ready-made CSV with new orders to demo ingestion
    GET  /api/export/<name>.csv   download a report as CSV
"""
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import io
import re
import tempfile
import threading
import time
from collections import OrderedDict
from functools import wraps
from pathlib import Path

import pandas as pd
from flask import Flask, Response, jsonify, request, send_from_directory

import config
from analytics.engine import DataNotReady, Filters, SalesAnalytics
from etl.generate_data import SalesGenerator
from etl.pipeline import run_pipeline
from storage import get_store

FRONTEND = Path(__file__).resolve().parent / "frontend"
app = Flask(__name__, static_folder=None)
app.config["MAX_CONTENT_LENGTH"] = config.MAX_UPLOAD_MB * 1024 * 1024
app.json.sort_keys = False

STORE = get_store()
ENGINE = SalesAnalytics(STORE)
_ingest_lock = threading.Lock()


# ----------------------------------------------------------------------------- tiny LRU response cache
class LRU:
    def __init__(self, size):
        self.size, self.d, self.lock = size, OrderedDict(), threading.Lock()
        self.hits = self.misses = 0

    def get(self, k):
        with self.lock:
            if k in self.d:
                self.d.move_to_end(k)
                self.hits += 1
                return self.d[k]
            self.misses += 1
            return None

    def put(self, k, v):
        with self.lock:
            self.d[k] = v
            if len(self.d) > self.size:
                self.d.popitem(last=False)

    def clear(self):
        with self.lock:
            self.d.clear()


CACHE = LRU(config.API_CACHE_SIZE)


def api(fn):
    """Adds response caching, query timing and uniform error handling to an endpoint."""
    @wraps(fn)
    def wrapper(*a, **kw):
        key = request.full_path
        hit = CACHE.get(key)
        if hit is not None:
            return jsonify({"data": hit["data"], "meta": {**hit["meta"], "cached": True, "query_ms": 0.1}})
        t0 = time.perf_counter()
        try:
            data = fn(*a, **kw)
        except DataNotReady as e:
            return jsonify({"error": str(e), "code": "NO_DATA"}), 503
        except (ValueError, KeyError) as e:
            return jsonify({"error": str(e)}), 400
        ms = round((time.perf_counter() - t0) * 1000, 1)
        meta = {"query_ms": ms, "cached": False, "engine": "DuckDB (columnar, vectorised)",
                "rows_in_lake": ENGINE.meta()["rows"]}
        CACHE.put(key, {"data": data, "meta": meta})
        return jsonify({"data": data, "meta": meta})
    return wrapper


def _csv_list(name, allowed):
    vals = [v for v in request.args.get(name, "").split(",") if v]
    return [v for v in vals if v in allowed]          # whitelist: anything unknown is ignored


def get_filters() -> Filters:
    m = ENGINE.meta()
    try:
        d1 = dt.date.fromisoformat(request.args.get("from") or m["min_date"])
        d2 = dt.date.fromisoformat(request.args.get("to") or m["max_date"])
    except ValueError:
        raise ValueError("dates must be formatted YYYY-MM-DD")
    if d1 > d2:
        d1, d2 = d2, d1
    return Filters(d1, d2, _csv_list("region", m["regions"]), _csv_list("category", m["categories"]),
                   _csv_list("channel", m["channels"]))


# ----------------------------------------------------------------------------- pages
@app.get("/")
def index():
    return send_from_directory(FRONTEND, "index.html")


@app.get("/<path:path>")
def static_files(path):
    if path.startswith("api/"):
        return jsonify({"error": "not found"}), 404
    return send_from_directory(FRONTEND, path)


# ----------------------------------------------------------------------------- analytics endpoints
@app.get("/api/health")
def health():
    return jsonify({"status": "ok", "data_ready": ENGINE.ready, "storage": STORE.describe(),
                    "cache": {"entries": len(CACHE.d), "hits": CACHE.hits, "misses": CACHE.misses}})


@app.get("/api/meta")
@api
def meta():
    return {**ENGINE.meta(), "storage": STORE.describe()}


@app.get("/api/kpis")
@api
def kpis():
    return ENGINE.kpis(get_filters())


@app.get("/api/trend")
@api
def trend():
    grain = request.args.get("grain", "month")
    if grain not in ("day", "week", "month"):
        raise ValueError("grain must be day, week or month")
    return ENGINE.trend(get_filters(), grain)


@app.get("/api/breakdown/<dim>")
@api
def breakdown(dim):
    return ENGINE.breakdown(get_filters(), dim, int(request.args.get("limit", 10)))


@app.get("/api/matrix")
@api
def matrix():
    return ENGINE.matrix(get_filters())


@app.get("/api/top-products")
@api
def top_products():
    return ENGINE.top_products(get_filters(), int(request.args.get("n", 10)))


@app.get("/api/heatmap")
@api
def heatmap():
    return ENGINE.heatmap(get_filters())


@app.get("/api/rfm")
@api
def rfm():
    return ENGINE.rfm(get_filters())


@app.get("/api/cohorts")
@api
def cohorts():
    return ENGINE.cohorts(get_filters())


@app.get("/api/basket")
@api
def basket():
    return ENGINE.basket(get_filters())


@app.get("/api/forecast")
@api
def forecast():
    return ENGINE.forecast(get_filters(), int(request.args.get("horizon", 6)))


@app.get("/api/anomalies")
@api
def anomalies():
    return ENGINE.anomalies(get_filters())


# ----------------------------------------------------------------------------- cloud-storage view
@app.get("/api/lake")
def lake():
    objs = STORE.list_objects("")
    zones = {}
    for o in objs:
        z = o["key"].split("/", 1)[0] + "/"
        d = zones.setdefault(z, {"zone": z, "objects": 0, "bytes": 0})
        d["objects"] += 1
        d["bytes"] += o["size"]
    runs = sorted((o for o in objs if o["key"].startswith(config.ZONE_META + "runs/")), key=lambda o: o["key"])[-8:]
    history = [STORE.get_json(o["key"]) for o in reversed(runs)]
    curated = [o for o in objs if o["key"].startswith(config.ZONE_CURATED)]
    partitions = sorted({"/".join(o["key"].split("/")[1:3]) for o in curated})
    sample = [o for o in objs if o["key"].startswith(config.ZONE_CURATED)][-12:] + \
             [o for o in objs if o["key"].startswith(config.ZONE_RAW)][-4:]
    return jsonify({"storage": STORE.describe(), "zones": list(zones.values()), "total_objects": len(objs),
                    "total_bytes": sum(o["size"] for o in objs), "partitions": len(partitions),
                    "latest_run": STORE.get_json(config.ZONE_META + "latest_run.json"), "history": history,
                    "objects": sample[::-1]})


# ----------------------------------------------------------------------------- ingestion (the "live cloud" demo)
def _ingested_hashes():
    return set(STORE.get_json(config.ZONE_META + "ingested_hashes.json", []))


@app.post("/api/ingest")
def ingest():
    f = request.files.get("file")
    if not f or not f.filename.lower().endswith(".csv"):
        return jsonify({"error": "Please upload a .csv file"}), 400
    data = f.read()
    digest = hashlib.sha256(data).hexdigest()
    with _ingest_lock:
        if digest in _ingested_hashes():
            return jsonify({"error": "This exact file was already ingested (checksum match) - skipped to avoid "
                                     "double counting. This is how the pipeline stays idempotent."}), 409
        safe = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(f.filename).name)
        key = f"{config.ZONE_RAW}uploads/{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%S}_{safe}"
        t0 = time.perf_counter()
        STORE.put_bytes(data, key)                       # 1. land the file in cloud storage (bronze)
        report = run_pipeline([key], full=False, trigger="dashboard-upload", workers=1)   # 2. ETL -> curated
        if report["errors"]:
            return jsonify({"error": report["errors"][0]["error"], "stage": "etl"}), 422
        STORE.put_json(sorted(_ingested_hashes() | {digest}), config.ZONE_META + "ingested_hashes.json")
        ENGINE.refresh()                                  # 3. query engine sees the new Parquet files
        CACHE.clear()                                     # 4. drop stale API answers
        m = ENGINE.meta()
        return jsonify({"ok": True, "object_key": key, "report": report, "max_date": m["max_date"],
                        "rows_in_lake": m["rows"], "total_ms": round((time.perf_counter() - t0) * 1000)})


@app.get("/api/sample.csv")
def sample_csv():
    """A small CSV of *new* October-2026 orders (with a few dirty rows) so ingestion can be demoed live."""
    seed = int(request.args.get("seed", 7))
    gen = SalesGenerator(42, 150_000)
    frames = [df for _, df in gen.generate(3000, pd.Timestamp("2026-10-01"), pd.Timestamp("2026-10-22"),
                                           dirty=True, id_start=9_000_000 + seed * 10_000, seed_offset=seed,
                                           id_prefix="ORD")]
    out = pd.concat(frames).to_csv(index=False)
    return Response(out, mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=new_orders_oct2026_{seed}.csv"})


# ----------------------------------------------------------------------------- CSV export
@app.get("/api/export/<name>.csv")
def export(name):
    f = get_filters()
    try:
        if name == "monthly":
            rows = ENGINE.trend(f, "month")["series"]
        elif name == "top-products":
            rows = ENGINE.top_products(f, 100)
        elif name == "rfm":
            rows = ENGINE.rfm(f)["segments"]
        elif name in ("category", "region", "state", "city", "channel", "payment_method"):
            rows = ENGINE.breakdown(f, name, 1000)
        else:
            return jsonify({"error": "unknown export"}), 404
    except DataNotReady as e:
        return jsonify({"error": str(e)}), 503
    buf = io.StringIO()
    if rows:
        w = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={name}.csv"})


if __name__ == "__main__":
    print(f"\n  Sales Analytics Dashboard -> http://localhost:{config.PORT}   "
          f"[storage: {STORE.describe()['backend']}]\n")
    app.run(host="0.0.0.0", port=config.PORT, threaded=True)
