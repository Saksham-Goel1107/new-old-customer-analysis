import time
import random
import logging
from typing import Any, Callable

import gspread
from gspread.exceptions import APIError

logger = logging.getLogger("sheets_utils")


def _retry_call(fn: Callable, description: str, max_retries: int = 5, initial_delay: float = 1.0, backoff: float = 2.0, **kwargs) -> Any:
    """Retry wrapper for transient Google Sheets / gspread calls.

    Retries on APIError and generic Exception with exponential backoff and jitter.
    """
    delay = initial_delay
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except APIError as e:
            logger.warning("gspread APIError during %s (attempt %d/%d): %s", description, attempt, max_retries, e)
            if attempt == max_retries:
                logger.error("Max retries reached for %s", description)
                raise
        except Exception as e:
            # Catch network-related or other intermittent failures
            logger.warning("Exception during %s (attempt %d/%d): %s", description, attempt, max_retries, e)
            if attempt == max_retries:
                logger.error("Max retries reached for %s", description)
                raise

        # sleep with jitter
        sleep_time = delay + random.uniform(0, 0.25 * delay)
        time.sleep(sleep_time)
        delay *= backoff


def open_spreadsheet(gc: gspread.client.Client, key: str, max_retries: int = 5) -> gspread.Spreadsheet:
    return _retry_call(lambda: gc.open_by_key(key), f"open_by_key {key}", max_retries=max_retries)


def get_worksheet(spreadsheet: gspread.Spreadsheet, title: str, max_retries: int = 3) -> gspread.Worksheet:
    """Get worksheet by title; raises if not found."""
    return _retry_call(lambda: spreadsheet.worksheet(title), f"get_worksheet {title}", max_retries=max_retries)


def ensure_worksheet(spreadsheet: gspread.Spreadsheet, title: str, rows: int = 1000, cols: int = 20, max_retries: int = 3) -> gspread.Worksheet:
    """Return existing worksheet or create it if missing (with retries)."""
    try:
        return get_worksheet(spreadsheet, title, max_retries=max_retries)
    except Exception:
        logger.info("Worksheet %s not found, creating it", title)
        return _retry_call(lambda: spreadsheet.add_worksheet(title=title, rows=rows, cols=cols), f"add_worksheet {title}", max_retries=max_retries)


def clear_worksheet(ws: gspread.Worksheet, max_retries: int = 3) -> None:
    _retry_call(lambda: ws.clear(), f"clear {getattr(ws, 'title', '<worksheet>')}", max_retries=max_retries)


def append_rows(ws: gspread.Worksheet, data: list, value_input_option: str = 'USER_ENTERED', max_retries: int = 5) -> None:
    _retry_call(lambda: ws.append_rows(data, value_input_option=value_input_option), f"append_rows {getattr(ws, 'title', '<worksheet>')}", max_retries=max_retries)
