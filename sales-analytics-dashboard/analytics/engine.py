"""Analytics engine - DuckDB (vectorised, columnar, multi-threaded OLAP) querying the
Parquet files that live in the cloud-storage 'curated' zone.

All SQL runs inside the engine; Python only builds the WHERE clause and shapes the result.
Filters are always passed as bound parameters (never string-concatenated) -> SQL-injection safe.
"""
from __future__ import annotations

import datetime as dt
import decimal
import math
import threading
from dataclasses import dataclass, field

import duckdb
import pandas as pd

import config
from analytics.forecasting import detect_anomalies, forecast_monthly


@dataclass
class Filters:
    date_from: dt.date
    date_to: dt.date
    regions: list = field(default_factory=list)
    categories: list = field(default_factory=list)
    channels: list = field(default_factory=list)

    def shifted(self, years=-1) -> "Filters":
        sh = lambda d: (pd.Timestamp(d) + pd.DateOffset(years=years)).date()
        return Filters(sh(self.date_from), sh(self.date_to), self.regions, self.categories, self.channels)


def _norm(v):
    if isinstance(v, (dt.datetime, dt.date)):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


class DataNotReady(RuntimeError):
    pass


class SalesAnalytics:
    DIMS = {"category", "region", "state", "city", "channel", "payment_method"}

    def __init__(self, store):
        self.store = store
        self.con = duckdb.connect()
        self.con.execute(f"PRAGMA threads={max(2, config.ETL_WORKERS)}")
        self._lock = threading.Lock()
        self._meta = None
        self.ready = False
        self.refresh()

    # ------------------------------------------------------------------ lifecycle
    def refresh(self):
        """(Re)attach to the curated zone. Remote buckets are synced incrementally first."""
        with self._lock:
            self._meta = None
            root = self.store.materialize(config.ZONE_CURATED)
            has = any(p.suffix == ".parquet" for p in (root / config.ZONE_CURATED).rglob("*.parquet")) \
                if (root / config.ZONE_CURATED).exists() else False
            if not has:
                self.ready = False
                return
            glob = (root / config.ZONE_CURATED).as_posix() + "/**/*.parquet"
            self.con.execute(
                f"CREATE OR REPLACE VIEW sales AS SELECT * FROM read_parquet('{glob}', hive_partitioning=true, "
                f"union_by_name=true)")
            self.ready = True

    def _cur(self):
        if not self.ready:
            raise DataNotReady("No curated data yet. Run: python -m etl.generate_data && python -m etl.pipeline")
        return self.con.cursor()

    def _rows(self, sql, params=()):
        cur = self._cur()
        res = cur.execute(sql, list(params))
        cols = [d[0] for d in res.description]
        return [{c: _norm(v) for c, v in zip(cols, row)} for row in res.fetchall()]

    def _one(self, sql, params=()):
        r = self._rows(sql, params)
        return r[0] if r else {}

    # ------------------------------------------------------------------ SQL helpers
    @staticmethod
    def _where(f: Filters, with_dates=True):
        cl, p = [], []
        if with_dates:
            cl += ["year BETWEEN ? AND ?", "order_date BETWEEN ? AND ?"]      # year -> partition pruning
            p += [f.date_from.year, f.date_to.year, f.date_from, f.date_to]
        for col, vals in (("region", f.regions), ("category", f.categories), ("channel", f.channels)):
            if vals:
                cl.append(f"{col} IN ({','.join('?' * len(vals))})")
                p += list(vals)
        return ("WHERE " + " AND ".join(cl)) if cl else "", p

    # ------------------------------------------------------------------ metadata
    def meta(self):
        if self._meta:
            return self._meta
        r = self._one("SELECT MIN(order_date) AS min_date, MAX(order_date) AS max_date, COUNT(*) AS rows, "
                      "COUNT(DISTINCT customer_id) AS customers, COUNT(DISTINCT product_id) AS products FROM sales")
        plural = {"region": "regions", "category": "categories", "channel": "channels",
                  "payment_method": "payment_methods", "state": "states"}
        for col, key in plural.items():
            r[key] = [x[col] for x in self._rows(f"SELECT DISTINCT {col} FROM sales ORDER BY 1")]
        self._meta = r
        return r

    # ------------------------------------------------------------------ KPIs
    def _kpi_block(self, f):
        w, p = self._where(f)
        r = self._one(
            "SELECT COALESCE(SUM(revenue),0) AS revenue, COALESCE(SUM(profit),0) AS profit, "
            "COUNT(DISTINCT order_id) AS orders, COALESCE(SUM(quantity),0) AS units, "
            f"COUNT(DISTINCT customer_id) AS customers FROM sales {w}", p)
        r["aov"] = r["revenue"] / r["orders"] if r["orders"] else 0
        r["margin"] = r["profit"] / r["revenue"] if r["revenue"] else 0
        return r

    def _monthly(self, f):
        w, p = self._where(f)
        return self._rows(
            "SELECT date_trunc('month', order_date) AS period, SUM(revenue) AS revenue, SUM(profit) AS profit, "
            "COUNT(DISTINCT order_id) AS orders, SUM(quantity) AS units, COUNT(DISTINCT customer_id) AS customers "
            f"FROM sales {w} GROUP BY 1 ORDER BY 1", p)

    def kpis(self, f: Filters):
        cur, prev = self._kpi_block(f), self._kpi_block(f.shifted(-1))
        growth = {}
        for k in ("revenue", "profit", "orders", "units", "customers", "aov"):
            growth[k] = (cur[k] - prev[k]) / prev[k] if prev.get(k) else None
        growth["margin"] = (cur["margin"] - prev["margin"]) if prev.get("revenue") else None   # percentage points
        m = self._monthly(f)[-24:]
        spark = {k: [x[k] for x in m] for k in ("revenue", "profit", "orders", "units", "customers")}
        spark["aov"] = [x["revenue"] / x["orders"] if x["orders"] else 0 for x in m]
        spark["margin"] = [x["profit"] / x["revenue"] if x["revenue"] else 0 for x in m]
        return {"current": cur, "previous": prev, "growth": growth, "spark": spark}

    # ------------------------------------------------------------------ time series
    def trend(self, f: Filters, grain="month"):
        unit = {"day": "day", "week": "week", "month": "month"}[grain]
        w, p = self._where(f)
        sql = (f"SELECT date_trunc('{unit}', order_date) AS period, SUM(revenue) AS revenue, SUM(profit) AS profit, "
               f"COUNT(DISTINCT order_id) AS orders, SUM(quantity) AS units FROM sales {w} GROUP BY 1 ORDER BY 1")
        rows = self._rows(sql, p)
        win = {"day": 7, "week": 4, "month": 3}[grain]
        s = pd.Series([r["revenue"] for r in rows], dtype=float)
        ma = s.rolling(win, min_periods=1).mean()
        for r, v in zip(rows, ma):
            r["moving_avg"] = float(v)
        prev = []
        if grain == "month":
            pf = f.shifted(-1)
            w2, p2 = self._where(pf)
            prev = self._rows("SELECT date_trunc('month', order_date) AS period, SUM(revenue) AS revenue "
                              f"FROM sales {w2} GROUP BY 1 ORDER BY 1", p2)
            for r in prev:
                r["period"] = (pd.Timestamp(r["period"]) + pd.DateOffset(years=1)).strftime("%Y-%m-%d")
        return {"grain": grain, "series": rows, "previous_year": prev, "moving_avg_window": win}

    # ------------------------------------------------------------------ breakdowns
    def breakdown(self, f: Filters, dim: str, limit=10):
        if dim not in self.DIMS:
            raise ValueError("unknown dimension")
        w, p = self._where(f)
        return self._rows(
            f"SELECT {dim} AS name, SUM(revenue) AS revenue, SUM(profit) AS profit, COUNT(DISTINCT order_id) AS orders, "
            f"SUM(quantity) AS units, SUM(profit)/SUM(revenue) AS margin, "
            f"SUM(revenue)/SUM(SUM(revenue)) OVER () AS share FROM sales {w} GROUP BY 1 ORDER BY revenue DESC LIMIT {int(limit)}", p)

    def matrix(self, f: Filters):
        """Revenue by region x category (stacked chart)."""
        w, p = self._where(f)
        return self._rows(f"SELECT region, category, SUM(revenue) AS revenue FROM sales {w} GROUP BY 1,2 ORDER BY 1,2", p)

    def top_products(self, f: Filters, n=10):
        w, p = self._where(f)
        return self._rows(
            "SELECT product_name AS name, category, SUM(revenue) AS revenue, SUM(profit) AS profit, "
            "SUM(quantity) AS units, SUM(profit)/SUM(revenue) AS margin, AVG(discount_pct) AS avg_discount "
            f"FROM sales {w} GROUP BY 1,2 ORDER BY revenue DESC LIMIT {int(n)}", p)

    def heatmap(self, f: Filters):
        w, p = self._where(f)
        rows = self._rows(f"SELECT dow, month(order_date) AS month, SUM(revenue) AS revenue FROM sales {w} GROUP BY 1,2", p)
        grid = [[0.0] * 12 for _ in range(7)]
        for r in rows:
            grid[int(r["dow"])][int(r["month"]) - 1] = r["revenue"]
        return {"grid": grid, "days": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
                "months": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]}

    # ------------------------------------------------------------------ customer analytics
    def rfm(self, f: Filters):
        w, p = self._where(f)
        asof = f.date_to + dt.timedelta(days=1)
        sql = f"""
        WITH cust AS (
          SELECT customer_id, date_diff('day', MAX(order_date), ?::DATE) AS recency,
                 COUNT(DISTINCT order_id) AS frequency, SUM(revenue) AS monetary
          FROM sales {w} GROUP BY 1),
        scored AS (
          SELECT *, 6 - NTILE(5) OVER (ORDER BY recency, customer_id) AS r,
                 NTILE(5) OVER (ORDER BY frequency, customer_id) AS f,
                 NTILE(5) OVER (ORDER BY monetary, customer_id) AS m FROM cust),
        seg AS (
          SELECT *, CASE
            WHEN r>=4 AND f>=4 THEN 'Champions'
            WHEN r>=3 AND f>=4 THEN 'Loyal Customers'
            WHEN r>=4 AND f>=2 THEN 'Potential Loyalists'
            WHEN r>=4 AND f=1 THEN 'New Customers'
            WHEN r=3 THEN 'Needs Attention'
            WHEN r<=2 AND f>=4 THEN 'Cant Lose Them'
            WHEN r<=2 AND f=3 THEN 'At Risk'
            WHEN r=1 AND f<=2 THEN 'Lost'
            ELSE 'Hibernating' END AS segment FROM scored)
        SELECT segment, COUNT(*) AS customers, SUM(monetary) AS revenue, AVG(recency) AS avg_recency_days,
               AVG(frequency) AS avg_orders, AVG(monetary) AS avg_spend
        FROM seg GROUP BY 1 ORDER BY revenue DESC"""
        rows = self._rows(sql, [asof] + p)
        tot = sum(r["customers"] for r in rows) or 1
        trev = sum(r["revenue"] for r in rows) or 1
        for r in rows:
            r["customer_share"] = r["customers"] / tot
            r["revenue_share"] = r["revenue"] / trev
        return {"segments": rows, "total_customers": tot, "as_of": asof.isoformat()}

    def cohorts(self, f: Filters, months=12):
        w, p = self._where(f, with_dates=False)
        sql = f"""
        WITH act AS (SELECT customer_id, date_trunc('month', order_date) AS m FROM sales {w} GROUP BY 1,2),
        fst AS (SELECT customer_id, MIN(m) AS fm FROM act GROUP BY 1)
        SELECT fst.fm AS cohort, date_diff('month', fst.fm, act.m) AS k, COUNT(*) AS n
        FROM act JOIN fst USING (customer_id)
        WHERE fst.fm >= date_trunc('month', ?::DATE) AND fst.fm <= date_trunc('month', ?::DATE)
          AND date_diff('month', fst.fm, act.m) <= 11
        GROUP BY 1,2 ORDER BY 1,2"""
        rows = self._rows(sql, p + [f.date_from, f.date_to])
        by = {}
        for r in rows:
            by.setdefault(r["cohort"], {})[int(r["k"])] = r["n"]
        last = pd.Timestamp(self.meta()["max_date"]).to_period("M")
        out = []
        for c in sorted(by)[-months:]:
            size = by[c].get(0, 0)
            span = (last - pd.Timestamp(c).to_period("M")).n
            ret = [(by[c].get(k, 0) / size if size and k <= span else None) for k in range(12)]
            out.append({"cohort": c[:7], "size": size, "retention": ret})
        return {"cohorts": out}

    def basket(self, f: Filters, n=12):
        w, p = self._where(f)
        sql = f"""
        WITH s AS (SELECT order_id, product_name FROM sales {w}),
        tot AS (SELECT COUNT(DISTINCT order_id) AS total FROM s),
        pc AS (SELECT product_name, COUNT(DISTINCT order_id) AS cnt FROM s GROUP BY 1),
        pairs AS (SELECT a.product_name AS a, b.product_name AS b, COUNT(*) AS together
                  FROM s a JOIN s b ON a.order_id=b.order_id AND a.product_name < b.product_name GROUP BY 1,2)
        SELECT a, b, together, together*1.0/tot.total AS support,
               together*1.0/pa.cnt AS confidence_a_to_b, together*1.0/pb.cnt AS confidence_b_to_a,
               (together*1.0/tot.total)/((pa.cnt*1.0/tot.total)*(pb.cnt*1.0/tot.total)) AS lift
        FROM pairs JOIN pc pa ON pa.product_name=pairs.a JOIN pc pb ON pb.product_name=pairs.b CROSS JOIN tot
        WHERE together >= GREATEST(20, tot.total*0.0002) ORDER BY lift DESC LIMIT {int(n)}"""
        return {"rules": self._rows(sql, p)}

    # ------------------------------------------------------------------ predictive
    def forecast(self, f: Filters, horizon=6):
        rows = self._monthly(f)
        if rows:                                   # drop partial first / last month
            last_end = (pd.Timestamp(rows[-1]["period"]) + pd.offsets.MonthEnd(0)).date()
            if f.date_to < last_end:
                rows = rows[:-1]
        if rows and f.date_from > pd.Timestamp(rows[0]["period"]).date():
            rows = rows[1:]
        return forecast_monthly([r["period"] for r in rows], [r["revenue"] for r in rows], horizon)

    def anomalies(self, f: Filters):
        w, p = self._where(f)
        rows = self._rows(f"SELECT order_date AS d, SUM(revenue) AS revenue FROM sales {w} GROUP BY 1 ORDER BY 1", p)
        return detect_anomalies([r["d"] for r in rows], [r["revenue"] for r in rows])
