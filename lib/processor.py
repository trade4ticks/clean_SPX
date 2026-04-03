"""
Core processing logic: read a parquet file, add computed fields and flags,
write back atomically.
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import DATA_DIR
from lib.fields import (
    add_bdte,
    add_datetime_fields,
    add_dte,
    add_gamma,
    add_intrinsic_extrinsic,
    add_moneyness,
    add_pricing_fields,
)
from lib.flags import add_flags

log = logging.getLogger(__name__)

# Raw columns that must be present (from step 1)
_RAW_COLUMNS = [
    "timestamp", "strike", "right", "settlement",
    "bid", "ask", "delta", "theta", "vega", "rho",
    "implied_vol", "underlying_price",
]


def _parquet_path(trading_date: str, expiration: str, settlement: str) -> Path:
    exp = expiration.replace("-", "")
    return Path(DATA_DIR) / trading_date / exp / f"{settlement}.parquet"


def _parse_expiration_date(expiration: str) -> date:
    """Parse YYYYMMDD or YYYY-MM-DD string to date."""
    clean = expiration.replace("-", "")
    return datetime.strptime(clean, "%Y%m%d").date()


def process_file(trading_date: str, expiration: str, settlement: str) -> int:
    """
    Process a single parquet file: add fields, add flags, write back.

    Returns the number of rows processed, or 0 if file not found / empty.
    """
    path = _parquet_path(trading_date, expiration, settlement)
    if not path.exists():
        return 0

    df = pd.read_parquet(path)
    if df.empty:
        return 0

    # Verify raw columns are present
    missing = [c for c in _RAW_COLUMNS if c not in df.columns]
    if missing:
        log.warning("Skipping %s — missing raw columns: %s", path, missing)
        return 0

    exp_date = _parse_expiration_date(expiration)

    # Add computed fields (order matters — some depend on others)
    df = add_datetime_fields(df)
    df = add_dte(df, exp_date)
    df = add_bdte(df, exp_date)
    df = add_pricing_fields(df)
    df = add_intrinsic_extrinsic(df)
    df = add_moneyness(df)
    df = add_gamma(df)

    # Add flags (depends on computed fields above)
    df = add_flags(df)

    # Atomic write: temp file then rename
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        suffix=".parquet",
        dir=str(path.parent),
    )
    os.close(fd)
    try:
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, tmp_path, compression="snappy")
        # On Windows, must remove target before rename
        if path.exists():
            path.unlink()
        Path(tmp_path).rename(path)
    except Exception:
        # Clean up temp file on failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    return len(df)


def discover_files(trading_date: str) -> list[tuple[str, str, str]]:
    """
    Discover all parquet files for a given trading date.

    Returns list of (trading_date, expiration, settlement) tuples.
    """
    day_dir = Path(DATA_DIR) / trading_date
    if not day_dir.exists():
        return []

    files = []
    for exp_dir in sorted(day_dir.iterdir()):
        if not exp_dir.is_dir():
            continue
        expiration = exp_dir.name  # YYYYMMDD
        for pq_file in sorted(exp_dir.glob("*.parquet")):
            settlement = pq_file.stem  # AM or PM
            files.append((trading_date, expiration, settlement))
    return files
