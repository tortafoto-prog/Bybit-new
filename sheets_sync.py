"""Write queue + state loader for Google Sheets.

A single shared instance serves all accounts. Each trade yields two rows over
its lifetime: an OPEN row (appended at entry, updated when SL/TP arrive) and a
CLOSE row (appended once fully closed). All writes go through a background flush
loop and are logged at INFO so the operator can see journaling happening.
"""

import asyncio
import logging

from models import InternalTrade, AccountState, PositionKey
from constants import TradeStatus, SHEETS_FLUSH_INTERVAL_S, SHEETS_BATCH_SIZE
from sheets_client import SheetsClient
from sheets_schema import trade_to_open_row, trade_to_close_row, open_row_to_trade

log = logging.getLogger(__name__)

# Column indices (0-based) in the 15-column schema
_C_TIMESTAMP = 0
_C_ACCOUNT = 1
_C_TICKET = 6
_C_IS_CLOSING = 9
_C_POSITION_ID = 11
_C_PLATFORM = 14


class SheetsSync:
    def __init__(self, client: SheetsClient):
        self.client = client
        self._queue: asyncio.Queue = asyncio.Queue()
        self._running = False
        self._open_row: dict[str, int] = {}    # trade_id -> OPEN row number
        self._closed: set[str] = set()         # trade_ids that already have a CLOSE row

    async def start(self):
        self._running = True
        asyncio.create_task(self._flush_loop())

    async def stop(self):
        self._running = False
        await self._drain()

    # ── public enqueue API ──────────────────────────────────────────────────
    async def enqueue_open(self, trade: InternalTrade):
        await self._queue.put(("open", trade))

    async def enqueue_update(self, trade: InternalTrade):
        await self._queue.put(("update", trade))

    async def enqueue_close(self, trade: InternalTrade):
        await self._queue.put(("close", trade))

    # ── flush loop ──────────────────────────────────────────────────────────
    async def _flush_loop(self):
        while self._running:
            try:
                await self._process_batch()
            except Exception as e:
                log.error(f"Sheets flush error: {e}", exc_info=True)
            await asyncio.sleep(SHEETS_FLUSH_INTERVAL_S)

    async def _drain(self):
        while not self._queue.empty():
            await self._process_batch()

    async def _process_batch(self):
        batch = []
        while len(batch) < SHEETS_BATCH_SIZE and not self._queue.empty():
            try:
                batch.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        for action, trade in batch:
            try:
                if action == "open":
                    await self._append_open(trade)
                elif action == "update":
                    await self._update_open(trade)
                elif action == "close":
                    await self._close(trade)
            except Exception as e:
                log.error(f"Sheets {action} failed for {trade.trade_id}: {e}")

    # ── write operations ────────────────────────────────────────────────────
    async def _append_open(self, trade: InternalTrade):
        if trade.trade_id in self._open_row:
            await self._update_open(trade)
            return
        row = await self.client.append_row(trade_to_open_row(trade))
        if row > 0:
            trade.sheets_open_row = row
            self._open_row[trade.trade_id] = row
        log.info(f"Sheets OPEN  {trade.trade_id} @ row {row or '?'}")

    async def _update_open(self, trade: InternalTrade):
        row = trade.sheets_open_row or self._open_row.get(trade.trade_id)
        if not row:
            await self._append_open(trade)
            return
        await self.client.update_row(row, trade_to_open_row(trade))
        log.info(f"Sheets UPDATE {trade.trade_id} @ row {row}")

    async def _close(self, trade: InternalTrade):
        await self._update_open(trade)            # refresh comment/SL on the open row
        if trade.trade_id in self._closed:
            return
        row = await self.client.append_row(trade_to_close_row(trade))
        if row > 0:
            trade.sheets_close_row = row
        self._closed.add(trade.trade_id)
        log.info(f"Sheets CLOSE {trade.trade_id} @ row {row or '?'} "
                 f"({trade.exit_type.value if trade.exit_type else '?'}, PnL={trade.pnl})")

    # ── startup state loading ───────────────────────────────────────────────
    async def load_open_trades(self, account_name: str, state: AccountState):
        """Populate `state` with still-open trades and the exec-id dedup set."""
        log.info(f"[{account_name}] Loading open trades from Sheets...")
        try:
            rows = await self.client.get_all_rows()
        except Exception as e:
            log.error(f"[{account_name}] Sheets read failed: {e}")
            return

        open_rows: dict[str, tuple[list, int]] = {}
        close_ids: set[str] = set()

        def g(row: list, i: int) -> str:
            return str(row[i]).strip() if i < len(row) and row[i] is not None else ""

        for idx, row in enumerate(rows):
            row_num = idx + 2  # +1 header, +1 to 1-based
            platform = g(row, _C_PLATFORM).lower()
            if platform not in ("bybit", ""):
                continue
            trade_id = g(row, _C_POSITION_ID)
            if not trade_id:
                continue
            is_closing = g(row, _C_IS_CLOSING).lower()

            if is_closing == "no":
                open_rows[trade_id] = (row, row_num)
                self._open_row[trade_id] = row_num
            elif is_closing == "yes":
                close_ids.add(trade_id)
                self._closed.add(trade_id)

            # dedup exec-ids for this account only
            if g(row, _C_ACCOUNT) == account_name:
                for eid in g(row, _C_TICKET).split(","):
                    eid = eid.strip()
                    if eid:
                        state.seen_exec_ids.add(eid)

        from constants import PositionMode
        hedge = state.position_mode == PositionMode.HEDGE

        loaded = 0
        for trade_id, (row, row_num) in open_rows.items():
            if g(row, _C_ACCOUNT) != account_name:
                continue
            try:
                trade = open_row_to_trade(row, row_num, hedge=hedge)
            except Exception as e:
                log.error(f"[{account_name}] Bad OPEN row {row_num}: {e}")
                continue
            state.last_exec_time_ms = max(state.last_exec_time_ms, trade.entry_time_ms)
            if trade_id in close_ids:
                continue  # already closed elsewhere in the sheet
            state.open_trades[trade.trade_id] = trade
            if trade.status == TradeStatus.PENDING:
                state.pending_sl_queue.setdefault(trade.pos_key, []).append(trade.trade_id)
                state.pending_tp_queue.setdefault(trade.pos_key, []).append(trade.trade_id)
            loaded += 1

        log.info(f"[{account_name}] Loaded {loaded} open trades, "
                 f"{len(state.seen_exec_ids)} exec_ids for dedup")
