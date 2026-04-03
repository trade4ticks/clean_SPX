"""
process_historical.py — Add computed fields and quality flags to historical
SPX options parquet files for a given date range.

Usage:
    python process_historical.py

You will be prompted for start and end dates (YYYYMMDD).

Processes files in-place: reads each parquet, adds columns, writes back
atomically. Safe to re-run — fields are recomputed each time.
"""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime

from tqdm import tqdm

from config import DATA_DIR
from lib.market_hours import get_trading_days, last_trading_day
from lib.processor import discover_files, process_file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def _parse_date(s: str) -> date:
    s = s.strip().replace("-", "")
    return datetime.strptime(s, "%Y%m%d").date()


def main() -> None:
    print("\n=== SPX Options — Clean, Flag & Add Fields ===")
    print(f"Data directory: {DATA_DIR}\n")

    while True:
        raw_start = input("Start date (YYYYMMDD): ").strip()
        try:
            start = _parse_date(raw_start)
            break
        except ValueError:
            print("  Invalid format. Use YYYYMMDD (e.g. 20240101)")

    while True:
        raw_end = input("End date   (YYYYMMDD): ").strip()
        try:
            end = _parse_date(raw_end)
            break
        except ValueError:
            print("  Invalid format. Use YYYYMMDD (e.g. 20241231)")

    if end < start:
        print("End date must be >= start date.")
        sys.exit(1)

    end = min(end, last_trading_day())
    if end < start:
        print("No completed trading days in the requested range.")
        sys.exit(0)

    trading_days = get_trading_days(start, end)
    print(f"\nRange: {start} -> {end}  ({len(trading_days)} trading days)\n")

    grand_total_rows = 0
    grand_total_files = 0
    grand_errors = 0

    for day_idx, day in enumerate(trading_days):
        day_str = day.strftime("%Y%m%d")
        day_label = f"[{day_idx + 1}/{len(trading_days)}] {day}"

        files = discover_files(day_str)
        if not files:
            log.info("%s  no files found — skipping", day_label)
            continue

        day_rows = 0
        day_files = 0
        day_errors = 0

        with tqdm(total=len(files), unit="file", ncols=90,
                  desc=f"  {day_str}") as pbar:
            for trading_date, expiration, settlement in files:
                try:
                    rows = process_file(trading_date, expiration, settlement)
                    day_rows += rows
                    if rows > 0:
                        day_files += 1
                    pbar.set_postfix_str(f"{expiration}/{settlement} ({rows}r)")
                except Exception as exc:
                    day_errors += 1
                    log.warning("FAILED  %s/%s/%s: %s",
                                trading_date, expiration, settlement, exc)
                finally:
                    pbar.update(1)

        grand_total_rows += day_rows
        grand_total_files += day_files
        grand_errors += day_errors

        log.info("%s  %s rows in %d files%s",
                 day_label, f"{day_rows:,}", day_files,
                 f"  ({day_errors} errors)" if day_errors else "")

    print(f"\n{'=' * 60}")
    print(f"Done. {grand_total_rows:,} rows processed across {grand_total_files:,} files. "
          f"{grand_errors} errors across {len(trading_days)} trading days.")


if __name__ == "__main__":
    main()
