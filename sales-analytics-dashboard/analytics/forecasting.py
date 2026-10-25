"""Statistical models used by the dashboard (pure NumPy - no heavy ML dependency).

1. forecast_monthly  : log-linear trend x seasonal index (multiplicative decomposition)
2. detect_anomalies  : robust z-score (median / MAD) on de-seasonalised daily sales
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def forecast_monthly(periods: list[str], values: list[float], horizon: int = 6) -> dict:
    """Forecast `horizon` months ahead. periods are 'YYYY-MM-01' strings."""
    y = np.asarray(values, dtype=float)
    n = len(y)
    if n < 12:
        raise ValueError("Need at least 12 full months of history to forecast. Widen the date range.")
    dates = pd.to_datetime(periods)
    t = np.arange(n)
    months = dates.month.values
    season = np.ones(12)
    # Back-fitting: alternate between (1) fitting the growth trend on de-seasonalised data and
    # (2) re-estimating the seasonal index. Without this, strong seasonal peaks bias the trend slope.
    for _ in range(8 if n >= 24 else 1):                # seasonality needs >= 2 full cycles
        b, a = np.polyfit(t, np.log(y / season[months - 1]), 1)      # exponential growth trend
        trend = np.exp(a + b * t)
        if n < 24:
            break
        ratio = y / trend
        season = np.array([ratio[months == m].mean() if (months == m).any() else 1.0 for m in range(1, 13)])
        season = season / season.mean()
    fitted = trend * season[months - 1]
    resid = (y - fitted) / fitted
    sigma = float(resid.std(ddof=1)) if n > 2 else 0.0
    mape = float(np.mean(np.abs(y - fitted) / y))
    fut_dates = pd.date_range(dates[-1] + pd.offsets.MonthBegin(1), periods=horizon, freq="MS")
    tf = np.arange(n, n + horizon)
    base = np.exp(a + b * tf) * season[fut_dates.month.values - 1]
    z = 1.96
    return {
        "history": [{"period": d.strftime("%Y-%m-%d"), "actual": float(v), "fitted": float(f)}
                    for d, v, f in zip(dates, y, fitted)],
        "forecast": [{"period": d.strftime("%Y-%m-%d"), "value": float(v),
                      "lower": float(v * (1 - z * sigma)), "upper": float(v * (1 + z * sigma))}
                     for d, v in zip(fut_dates, base)],
        "seasonality": [{"month": m + 1, "index": float(season[m])} for m in range(12)],
        "stats": {"annual_growth_pct": float(np.exp(12 * b) - 1), "mape": mape, "sigma": sigma, "months_used": n,
                  "horizon_total": float(base.sum()), "seasonal": bool(n >= 24)},
        "model": "Log-linear trend x seasonal index (multiplicative decomposition), 95% interval",
    }


def detect_anomalies(dates: list[str], values: list[float], window: int = 29, thresh: float = 3.5) -> dict:
    """Flag days whose sales are unusual after removing the weekday pattern."""
    s = pd.Series(values, index=pd.to_datetime(dates), dtype=float)
    if len(s) < 30:
        return {"series": [], "anomalies": [], "stats": {"flagged": 0}}
    dow_factor = s.groupby(s.index.dayofweek).mean() / s.mean()
    adj = s / s.index.dayofweek.map(dow_factor).values
    med = adj.rolling(window, center=True, min_periods=10).median()
    mad = (adj - med).abs().rolling(window, center=True, min_periods=10).median()
    scale = np.maximum(mad.values * 1.4826, med.values * 0.02)       # floor avoids divide-by-tiny
    z = (adj.values - med.values) / scale
    flag = np.abs(z) > thresh
    series = [{"date": d.strftime("%Y-%m-%d"), "revenue": float(v), "baseline": float(m * dow_factor[d.dayofweek]),
               "z": float(zz), "anomaly": bool(f)}
              for d, v, m, zz, f in zip(s.index, s.values, med.values, z, flag)]
    anomalies = []
    for row in series:
        if row["anomaly"]:
            dev = row["revenue"] / row["baseline"] - 1 if row["baseline"] else 0
            anomalies.append({**row, "type": "spike" if row["z"] > 0 else "drop", "deviation_pct": float(dev)})
    anomalies.sort(key=lambda r: -abs(r["z"]))
    return {"series": series, "anomalies": anomalies[:25],
            "stats": {"flagged": len(anomalies), "days": len(series), "threshold": thresh,
                      "spikes": sum(a["type"] == "spike" for a in anomalies),
                      "drops": sum(a["type"] == "drop" for a in anomalies)},
            "method": "Robust z-score = (x - rolling median) / (1.4826 x MAD) on weekday-adjusted daily revenue"}
