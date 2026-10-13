"""Independent numerical checks for the daily metric contract."""

import math
import statistics

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from marketdata.metrics import OUTPUT_COLUMNS, calculate_metrics


def make_frame(prices, symbol="ALFA", start="2026-01-01"):
    return pd.DataFrame(
        {
            "symbol": symbol,
            "date": pd.date_range(start, periods=len(prices), freq="B"),
            "adjusted_close": prices,
        }
    )


def test_matches_independent_price_return_and_sample_variance_math():
    prices = [100 + index * 0.7 + (index % 4) * 2.3 for index in range(35)]
    output = calculate_metrics(make_frame(prices))
    returns = [prices[i] / prices[i - 1] - 1 for i in range(1, len(prices))]
    assert list(output.columns) == OUTPUT_COLUMNS
    for index, row in output.iterrows():
        if index == 0:
            assert math.isnan(row.return_1d)
        else:
            assert row.return_1d == pytest.approx(returns[index - 1])
        if index < 19:
            assert math.isnan(row.sma_20)
        else:
            assert row.sma_20 == pytest.approx(sum(prices[index - 19 : index + 1]) / 20)
        if index < 20:
            assert math.isnan(row.volatility_20)
        else:
            expected = statistics.stdev(returns[index - 20 : index]) * math.sqrt(252)
            assert row.volatility_20 == pytest.approx(expected)
        assert row.drawdown == pytest.approx(prices[index] / max(prices[: index + 1]) - 1)


def test_drawdown_uses_running_peak_and_does_not_read_future_highs():
    output = calculate_metrics(make_frame([100, 80, 90, 120, 60]))
    assert output.drawdown.tolist() == pytest.approx([0, -0.2, -0.1, 0, -0.5])


def test_symbols_are_independent_and_input_order_does_not_change_results():
    alpha = make_frame([100, 110, 99], symbol="ALFA")
    beta = make_frame([10, 5, 10], symbol="BETA")
    unsorted = pd.concat([beta, alpha], ignore_index=True).sample(frac=1, random_state=4)
    output = calculate_metrics(unsorted)
    expected = pd.concat([calculate_metrics(alpha), calculate_metrics(beta)], ignore_index=True)
    assert_frame_equal(output, expected)
    assert math.isnan(output.loc[3, "return_1d"])
    assert output.loc[4, "return_1d"] == pytest.approx(-0.5)


def test_windows_use_observed_rows_even_with_large_calendar_gaps():
    frame = make_frame(range(1, 22))
    frame["date"] = pd.date_range("2023-01-01", periods=21, freq="MS")
    output = calculate_metrics(frame)
    assert output.loc[19, "sma_20"] == pytest.approx(10.5)
    assert math.isnan(output.loc[19, "volatility_20"])
    assert math.isfinite(output.loc[20, "volatility_20"])


def test_appending_future_observations_cannot_change_past_results():
    frame = make_frame([80 + (index % 5) * 7 for index in range(42)])
    full = calculate_metrics(frame)
    prefix = calculate_metrics(frame.iloc[:27])
    assert_frame_equal(full.iloc[:27], prefix)


def test_price_scaling_preserves_returns_volatility_and_drawdown():
    frame = make_frame([90 + index * 3 + (index % 3) * 5 for index in range(28)])
    original = calculate_metrics(frame)
    scaled = frame.copy()
    scaled["adjusted_close"] *= 17
    result = calculate_metrics(scaled)
    for column in ("return_1d", "volatility_20", "drawdown"):
        np.testing.assert_allclose(result[column], original[column], atol=1e-13, equal_nan=True)
    np.testing.assert_allclose(result.sma_20, original.sma_20 * 17, equal_nan=True)


def test_constant_prices_have_zero_returns_volatility_and_drawdown():
    output = calculate_metrics(make_frame([123.45] * 25))
    assert (output.return_1d.iloc[1:] == 0).all()
    assert (output.volatility_20.iloc[20:] == 0).all()
    assert (output.drawdown == 0).all()


def test_does_not_mutate_input_and_ignores_extra_columns():
    frame = make_frame([100, 101])
    frame["provider_notes"] = ["first", "second"]
    before = frame.copy(deep=True)
    output = calculate_metrics(frame)
    assert_frame_equal(frame, before)
    assert "provider_notes" not in output


def test_empty_valid_frame_has_stable_schema():
    output = calculate_metrics(pd.DataFrame(columns=["symbol", "date", "adjusted_close"]))
    assert output.empty
    assert output.columns.tolist() == OUTPUT_COLUMNS
    assert pd.api.types.is_datetime64_any_dtype(output.date)


@pytest.mark.parametrize("price", [0, -1, np.inf, -np.inf, np.nan, None, "oops", True, 1j])
def test_rejects_invalid_prices(price):
    frame = make_frame([100, price])
    with pytest.raises(ValueError, match="prices"):
        calculate_metrics(frame)


@pytest.mark.parametrize("invalid_date", ["2026-02-30", "not-a-date", None, pd.NaT, 123])
def test_rejects_invalid_dates(invalid_date):
    frame = make_frame([100])
    frame["date"] = [invalid_date]
    with pytest.raises(ValueError, match="Dates"):
        calculate_metrics(frame)


@pytest.mark.parametrize("symbol", ["", "  ", None, 123])
def test_rejects_invalid_symbols(symbol):
    frame = make_frame([100])
    frame["symbol"] = [symbol]
    with pytest.raises(ValueError, match="Symbols"):
        calculate_metrics(frame)


def test_rejects_duplicates_after_daily_date_normalization():
    frame = pd.DataFrame(
        {
            "symbol": ["ALFA", "ALFA"],
            "date": ["2026-01-01", "2026-01-01T10:30:00Z"],
            "adjusted_close": [100, 101],
        }
    )
    with pytest.raises(ValueError, match="Duplicate"):
        calculate_metrics(frame)


@pytest.mark.parametrize("column", ["symbol", "date", "adjusted_close"])
def test_rejects_missing_required_columns(column):
    with pytest.raises(ValueError, match="Missing required"):
        calculate_metrics(make_frame([100]).drop(columns=[column]))


def test_rejects_duplicate_column_names():
    frame = make_frame([100])
    frame = pd.concat([frame, frame[["symbol"]]], axis=1)
    with pytest.raises(ValueError, match="column names"):
        calculate_metrics(frame)
