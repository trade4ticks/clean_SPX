"""
process_intraday.py — Add computed fields and quality flags to today's
SPX options parquet files.

Designed to run via cron every 5 minutes after the intraday fetch completes.
Re-processes all of today's files each run (takes seconds).

Cron example (run 1 minute after the fetch to ensure data is written):
  1-59/5 9-16 * * 1-5  /path/to/venv/bin/python /path/to/process_intraday.py

Safe to call any time — exits immediately outside market hours.
"""

from __future__ import annotations

import logging
import sys
from datetime import time

from lib.market_hours import is_trading_day, now_et
from lib.processor import discover_files, process_file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Allow processing up to 16:05 to catch the final bar
_OPEN_TIME = time(9, 35)
_CLOSE_TIME = time(17, 0)


def main() -> None:
    now = now_et()

    if not is_trading_day(now.date()):
        log.info("Not a trading day — nothing to do.")
        sys.exit(0)

    t = now.time()
    if t < _OPEN_TIME or t > _CLOSE_TIME:
        log.info("Outside market hours (%s ET) — nothing to do.", now.strftime("%H:%M:%S"))
        sys.exit(0)

    today_str = now.date().strftime("%Y%m%d")
    log.info("Processing today's files (%s)", today_str)

    files = discover_files(today_str)
    if not files:
        log.info("No files found for %s", today_str)
        sys.exit(0)

    total_rows = 0
    total_files = 0
    errors = 0

    for trading_date, expiration, settlement in files:
        try:
            rows = process_file(trading_date, expiration, settlement)
            total_rows += rows
            if rows > 0:
                total_files += 1
        except Exception as exc:
            errors += 1
            log.warning("FAILED  %s/%s/%s: %s",
                        trading_date, expiration, settlement, exc)

    log.info("Done. %s rows across %d files. %d errors.",
             f"{total_rows:,}", total_files, errors)


if __name__ == "__main__":
    main()
