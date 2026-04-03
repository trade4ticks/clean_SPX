"""
Data quality flags for SPX options data.

Each flag function adds a boolean column. All thresholds come from
the FLAG_THRESHOLDS dict (loaded from flag_config.yaml).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from config import FLAG_THRESHOLDS


def add_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Add all flag columns and a composite flag_any."""
    t = FLAG_THRESHOLDS

    df["flag_wide_spread_abs"] = df["spread"] > t["wide_spread_abs"]
    df["flag_wide_spread_pct"] = df["spread_pct"] > t["wide_spread_pct"]
    df["flag_negative_extrinsic"] = df["extrinsic"] < 0
    df["flag_crossed_market"] = df["bid"] > df["ask"]
    df["flag_zero_bid"] = df["bid"] == 0
    df["flag_iv_extreme_high"] = df["implied_vol"] > t["iv_extreme_high"]
    df["flag_iv_extreme_low"] = (df["implied_vol"] < t["iv_extreme_low"]) & (df["implied_vol"] > 0)
    df["flag_iv_missing"] = df["implied_vol"].isna()
    df["flag_delta_missing"] = df["delta"].isna()
    df["flag_deep_otm"] = (df["moneyness"] < t["deep_otm_lower"]) | (df["moneyness"] > t["deep_otm_upper"])
    df["flag_near_expiry_wide"] = (
        (df["dte"] <= t["near_expiry_dte"])
        & (df["spread_pct"] > t["near_expiry_spread_pct"])
    )

    # Stale underlying: same underlying_price for all rows at a given timestamp
    # Flag if underlying_price is identical across 3+ consecutive timestamps
    df["flag_stale_underlying"] = _detect_stale_underlying(df)

    # Composite flag
    flag_cols = [c for c in df.columns if c.startswith("flag_")]
    df["flag_any"] = df[flag_cols].any(axis=1)

    return df


def _detect_stale_underlying(df: pd.DataFrame) -> pd.Series:
    """
    Flag rows where the underlying_price has not changed for 3+ consecutive
    5-minute bars. Computed per (right, settlement) group to avoid cross-group
    contamination, but underlying is the same across rights so we just use
    unique timestamps.
    """
    result = pd.Series(False, index=df.index)

    # Get unique (timestamp, underlying_price) pairs
    ts_prices = (
        df[["timestamp", "underlying_price"]]
        .drop_duplicates(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    if len(ts_prices) < 3:
        return result

    prices = ts_prices["underlying_price"].values
    stale_ts = set()

    # Find runs of identical prices of length >= 3
    run_start = 0
    for i in range(1, len(prices) + 1):
        if i == len(prices) or prices[i] != prices[run_start]:
            if i - run_start >= 3:
                for j in range(run_start, i):
                    stale_ts.add(ts_prices.loc[j, "timestamp"])
            run_start = i

    if stale_ts:
        result = df["timestamp"].isin(stale_ts)

    return result
