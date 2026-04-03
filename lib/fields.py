"""
Computed fields for SPX options data.

All functions take a DataFrame (single parquet file's worth of data)
and return it with new columns added. Functions are idempotent — if
the columns already exist they are overwritten.
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal

_NYSE = mcal.get_calendar("NYSE")


def add_datetime_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Extract trade_date and quote_time from timestamp string."""
    ts = pd.to_datetime(df["timestamp"])
    df["trade_date"] = ts.dt.date
    df["quote_time"] = ts.dt.time
    return df


def add_dte(df: pd.DataFrame, expiration_date: date) -> pd.DataFrame:
    """Calendar days to expiration."""
    ts = pd.to_datetime(df["timestamp"])
    exp = pd.Timestamp(expiration_date)
    df["dte"] = (exp - ts.dt.normalize()).dt.days
    return df


def add_bdte(df: pd.DataFrame, expiration_date: date) -> pd.DataFrame:
    """Business (trading) days to expiration."""
    trade_dates = df["trade_date"].unique()
    bdte_map = {}
    for td in trade_dates:
        td_ts = pd.Timestamp(td)
        exp_ts = pd.Timestamp(expiration_date)
        if td_ts >= exp_ts:
            bdte_map[td] = 0
        else:
            schedule = _NYSE.schedule(
                start_date=td_ts.strftime("%Y-%m-%d"),
                end_date=exp_ts.strftime("%Y-%m-%d"),
            )
            # Subtract 1 because schedule includes start date
            bdte_map[td] = max(0, len(schedule) - 1)
    df["bdte"] = df["trade_date"].map(bdte_map)
    return df


def add_pricing_fields(df: pd.DataFrame) -> pd.DataFrame:
    """mid_price, spread, spread_pct."""
    df["mid_price"] = (df["bid"] + df["ask"]) / 2
    df["spread"] = df["ask"] - df["bid"]
    df["spread_pct"] = np.where(
        df["mid_price"] > 0,
        df["spread"] / df["mid_price"],
        np.nan,
    )
    return df


def add_intrinsic_extrinsic(df: pd.DataFrame) -> pd.DataFrame:
    """Intrinsic and extrinsic value based on right (C/P)."""
    is_call = df["right"] == "C"
    df["intrinsic"] = np.where(
        is_call,
        np.maximum(0, df["underlying_price"] - df["strike"]),
        np.maximum(0, df["strike"] - df["underlying_price"]),
    )
    df["extrinsic"] = df["mid_price"] - df["intrinsic"]
    return df


def add_moneyness(df: pd.DataFrame) -> pd.DataFrame:
    """moneyness = underlying / strike, plus log_moneyness."""
    df["moneyness"] = df["underlying_price"] / df["strike"]
    df["log_moneyness"] = np.log(df["moneyness"])
    return df


def add_gamma(df: pd.DataFrame) -> pd.DataFrame:
    """
    Numerical gamma via central finite difference of delta across strikes.

    Grouped by (timestamp, right, settlement) so each group is a single
    delta-vs-strike curve. At boundaries uses forward/backward difference.
    """
    df = df.sort_values(["timestamp", "right", "settlement", "strike"]).copy()
    df["gamma"] = np.nan

    group_cols = ["timestamp", "right", "settlement"]
    for _, grp in df.groupby(group_cols):
        idx = grp.index
        strikes = grp["strike"].values
        deltas = grp["delta"].values
        n = len(strikes)
        if n < 2:
            continue

        gamma = np.full(n, np.nan)
        # Central difference for interior points
        if n > 2:
            gamma[1:-1] = (deltas[2:] - deltas[:-2]) / (strikes[2:] - strikes[:-2])
        # Forward difference for first point
        gamma[0] = (deltas[1] - deltas[0]) / (strikes[1] - strikes[0])
        # Backward difference for last point
        gamma[-1] = (deltas[-1] - deltas[-2]) / (strikes[-1] - strikes[-2])

        df.loc[idx, "gamma"] = gamma

    return df
