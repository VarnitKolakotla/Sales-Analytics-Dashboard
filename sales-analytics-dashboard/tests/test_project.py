"""Fast tests (no 2M-row dataset needed):  pytest -q"""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analytics.forecasting import detect_anomalies, forecast_monthly
from etl.generate_data import SalesGenerator
from etl.pipeline import clean_dataframe
from storage.object_store import LocalObjectStore


@pytest.fixture(scope="module")
def raw():
    gen = SalesGenerator(1, 3000)
    return pd.concat([df for _, df in gen.generate(6000, pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-31"), dirty=True)])


def test_cleaning_rejects_bad_rows_and_keeps_good(raw):
    clean, rejected, counts = clean_dataframe(raw.astype(str).replace({"None": None, "nan": None}))
    assert len(clean) + len(rejected) == len(raw)
    assert len(rejected) > 0 and sum(counts.values()) == len(rejected)
    assert clean["order_id"].notna().all() and (clean["quantity"] >= 1).all() and (clean["unit_price"] > 0).all()
    assert not clean.duplicated(["order_id", "line_no"]).any()
    assert set(clean["region"]) <= {"North", "South", "East", "West", "Central"}


def test_metrics_are_consistent(raw):
    clean, _, _ = clean_dataframe(raw.astype(str).replace({"None": None, "nan": None}))
    assert ((clean["gross_amount"] - clean["discount_amount"] - clean["revenue"]).abs() < 0.02).all()
    assert ((clean["revenue"] - clean["cost"] - clean["profit"]).abs() < 0.02).all()


def test_missing_columns_raise():
    with pytest.raises(ValueError):
        clean_dataframe(pd.DataFrame({"order_id": ["a"]}))


def test_object_store_roundtrip_and_traversal(tmp_path):
    s = LocalObjectStore(tmp_path, "b")
    s.put_bytes(b"hello", "raw/x/a.txt")
    assert s.get_bytes("raw/x/a.txt") == b"hello" and s.exists("raw/x/a.txt")
    assert [o["key"] for o in s.list_objects("raw/")] == ["raw/x/a.txt"]
    with pytest.raises(ValueError):
        s.get_bytes("../../etc/passwd")
    assert s.delete_prefix("raw/") == 1 and not s.exists("raw/x/a.txt")


def test_forecast_recovers_growth_and_seasonality():
    import numpy as np
    months = pd.date_range("2023-01-01", periods=36, freq="MS")
    seas = np.array([1, .9, 1, 1, 1, .9, .9, 1, 1, 1.4, 1.4, 1.2])
    y = 100 * 1.02 ** np.arange(36) * seas[months.month - 1]
    f = forecast_monthly([d.strftime("%Y-%m-%d") for d in months], list(y), 6)
    assert f["stats"]["mape"] < 0.05 and 0.2 < f["stats"]["annual_growth_pct"] < 0.3
    assert max(f["seasonality"], key=lambda x: x["index"])["month"] in (10, 11)
    with pytest.raises(ValueError):
        forecast_monthly(["2025-01-01"] * 5, [1] * 5)


def test_anomaly_detector_finds_injected_outlier():
    import numpy as np
    d = pd.date_range("2025-01-01", periods=120)
    v = np.full(120, 100.0) + np.random.default_rng(0).normal(0, 2, 120)
    v[60] = 400
    r = detect_anomalies([x.strftime("%Y-%m-%d") for x in d], list(v))
    assert any(a["date"] == d[60].strftime("%Y-%m-%d") and a["type"] == "spike" for a in r["anomalies"])
