"""
Market hours utilities for US equity/options markets.

All datetime logic is in US/Eastern time.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

import pandas_market_calendars as mcal
import pytz

_ET = pytz.timezone("US/Eastern")
_NYSE = mcal.get_calendar("NYSE")


def now_et() -> datetime:
    return datetime.now(_ET)


def is_trading_day(d: date) -> bool:
    schedule = _NYSE.schedule(
        start_date=d.strftime("%Y-%m-%d"),
        end_date=d.strftime("%Y-%m-%d"),
    )
    return not schedule.empty


def get_trading_days(start: date, end: date) -> list[date]:
    """Return list of NYSE trading days between start and end (inclusive)."""
    schedule = _NYSE.schedule(
        start_date=start.strftime("%Y-%m-%d"),
        end_date=end.strftime("%Y-%m-%d"),
    )
    return [d.date() for d in schedule.index]


def last_trading_day(reference: date | None = None) -> date:
    """Return the most recent completed trading day at or before reference."""
    if reference is None:
        reference = now_et().date()
    d = reference
    for _ in range(7):
        if is_trading_day(d):
            return d
        d -= timedelta(days=1)
    return reference
