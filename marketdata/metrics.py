"""Point-in-time metrics from validated, adjusted daily prices.

Rolling windows count observed trading rows, not calendar days. Missing warm-up
values remain NaN so callers can distinguish unavailable metrics from zero.
"""

from __future__ import annotations

from datetime import date, datetime

import numpy as np
import pandas as pd

OUTPUT_COLUMNS = [
    "symbol",
    "date",
    "return_1d",
    "sma_20",
    "volatility_20",
    "drawdown",
]


def calculate_metrics(frame: pd.DataFrame) -> pd.DataFrame:
    """Return sorted daily metrics without mutating ``frame``.

    Required inputs are ``symbol``, ``date`` and ``adjusted_close``. Dates are
    normalized to UTC calendar days; duplicates are rejected after normalization.
    Prices must be numeric, finite and strictly positive. Return and drawdown are
    fractions (0.01 means 1%). Volatility is the sample standard deviation of 20
    daily returns, annualized by sqrt(252), requiring 21 observed prices.
    """
    required = {"symbol", "date", "adjusted_close"}
    if not frame.columns.is_unique:
        raise ValueError("Input column names must be unique")
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(sorted(missing))}")

    data = frame[["symbol", "date", "adjusted_close"]].copy()
    if not data["symbol"].map(lambda value: isinstance(value, str) and bool(value.strip())).all():
        raise ValueError("Symbols must be nonempty strings")

    valid_date_types = (str, date, datetime, pd.Timestamp, np.datetime64)
    if not data["date"].map(lambda value: isinstance(value, valid_date_types)).all():
        raise ValueError("Dates must be valid calendar dates")
    parsed_dates = pd.to_datetime(data["date"], errors="coerce", format="mixed", utc=True)
    if parsed_dates.isna().any():
        raise ValueError("Dates must be valid calendar dates")
    data["date"] = parsed_dates.dt.normalize().dt.tz_localize(None)

    if (
        data["adjusted_close"]
        .map(lambda value: isinstance(value, (bool, np.bool_, complex, np.complexfloating)))
        .any()
    ):
        raise ValueError("Adjusted prices must be finite, positive numbers")
    prices = pd.to_numeric(data["adjusted_close"], errors="coerce").astype(float)
    if not (np.isfinite(prices) & (prices > 0)).all():
        raise ValueError("Adjusted prices must be finite, positive numbers")
    data["adjusted_close"] = prices

    if data.duplicated(["symbol", "date"]).any():
        raise ValueError("Duplicate price for the same symbol and date")
    data = data.sort_values(["symbol", "date"], kind="stable").reset_index(drop=True)
    if data.empty:
        return pd.DataFrame(
            {
                "symbol": pd.Series(dtype="object"),
                "date": pd.Series(dtype="datetime64[ns]"),
                **{name: pd.Series(dtype="float64") for name in OUTPUT_COLUMNS[2:]},
            }
        )

    results = []
    for _, group in data.groupby("symbol", sort=False):
        close = group["adjusted_close"]
        daily_returns = close.pct_change(fill_method=None)
        result = group[["symbol", "date"]].copy()
        result["return_1d"] = daily_returns
        result["sma_20"] = close.rolling(20, min_periods=20).mean()
        result["volatility_20"] = daily_returns.rolling(20, min_periods=20).std(ddof=1) * np.sqrt(
            252
        )
        result["drawdown"] = close / close.cummax() - 1
        results.append(result)
    return pd.concat(results, ignore_index=True)[OUTPUT_COLUMNS]
