"""Google Sheets client (gspread) with rate limiting, retry and clear logging.

The previous version silently lost writes; this one logs every append/update
result and runs a one-time write self-test at startup so a missing
write-permission is obvious in the logs immediately.
"""

import asyncio
import logging
import time
from typing import Optional

import gspread
from google.oauth2.service_account import Credentials

from sheets_schema import HEADERS

log = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_MIN_INTERVAL_S = 1.1   # ~55 req/min, under Google's 60/min/user limit


class SheetsClient:
    def __init__(self, credentials_json: dict, sheet_id: str, sheet_name: str = "Trades"):
        self.sheet_id = sheet_id
        self.sheet_name = sheet_name
        self._creds_json = credentials_json
        self._gc: Optional[gspread.Client] = None
        self._ws: Optional[gspread.Worksheet] = None
        self._lock = asyncio.Lock()
        self._last_req = 0.0
        self.can_write = False

    async def connect(self):
        await asyncio.get_event_loop().run_in_executor(None, self._connect_sync)

    def _connect_sync(self):
        creds = Credentials.from_service_account_info(self._creds_json, scopes=SCOPES)
        self._gc = gspread.authorize(creds)
        try:
            ss = self._gc.open_by_key(self.sheet_id)
        except gspread.SpreadsheetNotFound:
            raise ValueError(
                f"Spreadsheet {self.sheet_id} not found — check SHEET_ID and that the "
                f"service account ({self._creds_json.get('client_email', '?')}) has access."
            )
        try:
            self._ws = ss.worksheet(self.sheet_name)
        except gspread.WorksheetNotFound:
            self._ws = ss.add_worksheet(title=self.sheet_name, rows=2000, cols=len(HEADERS))
            self._ws.update("A1", [HEADERS])

        existing = self._ws.row_values(1)
        if not existing:
            self._ws.update("A1", [HEADERS])
            log.info("Sheets: wrote header row")
        elif len(existing) < len(HEADERS):
            log.warning(f"Sheets: header has {len(existing)} cols, expected {len(HEADERS)}")

        # Diagnostic: how many real data rows are there, vs. where does an
        # append land? A big difference means trailing blank/junk rows are
        # pushing appends far below the visible data.
        try:
            col_a = self._ws.col_values(1)
            log.info(f"Sheets: column A last non-empty row = {len(col_a)}")
        except Exception:
            pass

        # Write self-test: append + delete a probe row so a permission problem
        # surfaces here rather than silently dropping every trade.
        try:
            probe = self._ws.append_row(
                ["__probe__"] + [""] * (len(HEADERS) - 1),
                value_input_option="USER_ENTERED", table_range="A1")
            rng = probe.get("updates", {}).get("updatedRange", "")
            row = _row_from_range(rng)
            log.info(f"Sheets: append self-test landed at row {row}")
            if row:
                self._ws.delete_rows(row)
            self.can_write = True
            log.info(f"Sheets connected and writable: {self.sheet_name}")
        except gspread.exceptions.APIError as e:
            self.can_write = False
            log.error(f"Sheets: NO WRITE ACCESS to '{self.sheet_name}' — {e}. "
                      f"Share the sheet with the service account as Editor.")

    async def _throttle(self):
        elapsed = time.time() - self._last_req
        if elapsed < _MIN_INTERVAL_S:
            await asyncio.sleep(_MIN_INTERVAL_S - elapsed)
        self._last_req = time.time()

    async def append_row(self, values: list) -> int:
        """Append a row, return its 1-based row number (0 if unknown)."""
        async with self._lock:
            await self._throttle()
            try:
                res = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self._ws.append_row(
                        values, value_input_option="USER_ENTERED", table_range="A1"),
                )
                row = _row_from_range(res.get("updates", {}).get("updatedRange", ""))
                return row
            except Exception as e:
                log.error(f"Sheets append failed: {e}")
                raise

    async def update_row(self, row_number: int, values: list):
        if row_number <= 1:
            return
        async with self._lock:
            await self._throttle()
            end_col = chr(ord("A") + len(values) - 1)
            rng = f"A{row_number}:{end_col}{row_number}"
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self._ws.update(rng, [values], value_input_option="USER_ENTERED"),
                )
            except Exception as e:
                log.error(f"Sheets update row={row_number} failed: {e}")
                raise

    async def get_all_rows(self) -> list[list]:
        async with self._lock:
            await self._throttle()
            values = await asyncio.get_event_loop().run_in_executor(
                None, self._ws.get_all_values)
            return values[1:] if len(values) > 1 else []


def _row_from_range(updated_range: str) -> int:
    """Extract the row number from a range like 'Trades!A532:O532'."""
    if "!" not in updated_range:
        return 0
    a1 = updated_range.split("!")[1].split(":")[0]
    digits = "".join(c for c in a1 if c.isdigit())
    return int(digits) if digits else 0
