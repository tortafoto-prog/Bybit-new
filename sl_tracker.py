"""Stop-loss / take-profit assignment to internal trades.

Rules:
- A new SL/TP change is assigned to the NEWEST pending trade for that position.
- If an SL arrives before its fill (WS race), it is buffered briefly and applied
  when the trade is registered.
- If no SL arrives within the grace period, the trade becomes INVALID (still
  volume-tracked, flagged in the sheet) — never silently dropped.
"""

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from models import AccountState, SLEvent, TPEvent, PositionKey, InternalTrade
from constants import TradeStatus, SL_GRACE_PERIOD_MS, SL_UNMATCHED_BUFFER_TTL_MS

if TYPE_CHECKING:
    from journal import Journal

log = logging.getLogger(__name__)


class SLTracker:
    def __init__(self, state: AccountState, journal: "Journal"):
        self.state = state
        self.journal = journal
        self._grace_tasks: dict[str, asyncio.Task] = {}
        self._buffer_tasks: dict[tuple[str, PositionKey], asyncio.Task] = {}

    # ── SL ──────────────────────────────────────────────────────────────────
    async def on_sl_change(self, ev: SLEvent):
        key = PositionKey(ev.symbol, ev.position_idx)
        if ev.sl_price == self.state.last_known_sl.get(key, 0.0):
            return
        self.state.last_known_sl[key] = ev.sl_price

        if ev.sl_price == 0:
            log.info(f"[{self.state.account_name}] SL removed on {key}")
            return

        pending = self.state.pending_sl_queue.get(key, [])
        if not pending:
            log.info(f"[{self.state.account_name}] SL {ev.sl_price} on {key} "
                     f"but no pending trade — buffering")
            self.state.recent_unmatched_sl[key] = ev
            self._schedule_buffer_cleanup("sl", key)
            return

        trade = self.state.open_trades.get(pending[-1])
        if trade is None:
            pending.pop()
            return
        if trade.sl_price is not None:
            self._pop(pending, key, "sl")
            return
        if not self._valid_sl_side(trade, ev.sl_price):
            log.warning(f"[{self.state.account_name}] SL {ev.sl_price} on wrong side of "
                        f"entry {trade.entry_price} ({trade.side}) — skipping")
            return

        trade.sl_price = ev.sl_price
        trade.sl_assigned_time_ms = ev.timestamp_ms
        trade.sl_source = ev.source
        if trade.status != TradeStatus.CLOSED:
            trade.status = TradeStatus.ACTIVE
        trade.updated_at_ms = int(time.time() * 1000)
        self._pop(pending, key, "sl")
        self._cancel_grace(trade.trade_id)
        log.info(f"[{self.state.account_name}] SL {ev.sl_price} → {trade.trade_id} "
                 f"(entry={trade.entry_price})")
        # SL is final → journal the OPEN now (with SL).
        await self.journal.post_open(trade)

    # ── TP ──────────────────────────────────────────────────────────────────
    async def on_tp_change(self, ev: TPEvent):
        key = PositionKey(ev.symbol, ev.position_idx)
        if ev.tp_price == self.state.last_known_tp.get(key, 0.0):
            return
        self.state.last_known_tp[key] = ev.tp_price

        if ev.tp_price == 0:
            return

        pending = self.state.pending_tp_queue.get(key, [])
        if not pending:
            self.state.recent_unmatched_tp[key] = ev
            self._schedule_buffer_cleanup("tp", key)
            return

        trade = self.state.open_trades.get(pending[-1])
        if trade is None:
            pending.pop()
            return
        if trade.tp_price is not None:
            self._pop(pending, key, "tp")
            return
        if not self._valid_tp_side(trade, ev.tp_price):
            return

        trade.tp_price = ev.tp_price
        trade.tp_assigned_time_ms = ev.timestamp_ms
        trade.tp_source = ev.source
        trade.updated_at_ms = int(time.time() * 1000)
        self._pop(pending, key, "tp")
        log.info(f"[{self.state.account_name}] TP {ev.tp_price} → {trade.trade_id}")
        # TP is captured in the OPEN payload when it is posted (on SL-resolve).

    # ── trade registration ──────────────────────────────────────────────────
    def register_pending_trade(self, trade: InternalTrade):
        key = trade.pos_key
        self.state.pending_sl_queue.setdefault(key, []).append(trade.trade_id)
        self.state.pending_tp_queue.setdefault(key, []).append(trade.trade_id)

        buffered_sl = self.state.recent_unmatched_sl.pop(key, None)
        if buffered_sl:
            log.info(f"[{self.state.account_name}] Applying buffered SL "
                     f"{buffered_sl.sl_price} to {trade.trade_id}")
            asyncio.create_task(self.on_sl_change(buffered_sl))

        buffered_tp = self.state.recent_unmatched_tp.pop(key, None)
        if buffered_tp:
            asyncio.create_task(self.on_tp_change(buffered_tp))

        if not buffered_sl:
            self.start_grace_timer(trade)

    # ── grace timer ─────────────────────────────────────────────────────────
    def start_grace_timer(self, trade: InternalTrade):
        grace_ms = SL_GRACE_PERIOD_MS
        if trade.grace_deadline_ms:
            remaining = trade.grace_deadline_ms - int(time.time() * 1000)
            if remaining <= 0:
                asyncio.create_task(self._grace_expired(trade.trade_id))
                return
            grace_ms = remaining

        async def timer():
            await asyncio.sleep(grace_ms / 1000.0)
            await self._grace_expired(trade.trade_id)

        self._grace_tasks[trade.trade_id] = asyncio.create_task(timer())

    async def _grace_expired(self, trade_id: str):
        self._grace_tasks.pop(trade_id, None)
        trade = self.state.open_trades.get(trade_id)
        if trade is None or trade.status != TradeStatus.PENDING:
            return
        trade.status = TradeStatus.INVALID
        trade.updated_at_ms = int(time.time() * 1000)
        pending = self.state.pending_sl_queue.get(trade.pos_key, [])
        if trade_id in pending:
            pending.remove(trade_id)
            if not pending:
                self.state.pending_sl_queue.pop(trade.pos_key, None)
        log.warning(f"[{self.state.account_name}] {trade_id} INVALID: no SL in grace period")
        # Grace expired with no SL → journal the OPEN now (web app flags it HIBA!).
        await self.journal.post_open(trade)

    def _cancel_grace(self, trade_id: str):
        task = self._grace_tasks.pop(trade_id, None)
        if task:
            task.cancel()

    # ── helpers ─────────────────────────────────────────────────────────────
    def _pop(self, pending: list, key: PositionKey, kind: str):
        if pending:
            pending.pop()
        if not pending:
            (self.state.pending_sl_queue if kind == "sl"
             else self.state.pending_tp_queue).pop(key, None)

    @staticmethod
    def _valid_sl_side(trade: InternalTrade, sl: float) -> bool:
        return sl < trade.entry_price if trade.side == "Buy" else sl > trade.entry_price

    @staticmethod
    def _valid_tp_side(trade: InternalTrade, tp: float) -> bool:
        return tp > trade.entry_price if trade.side == "Buy" else tp < trade.entry_price

    def _schedule_buffer_cleanup(self, kind: str, key: PositionKey):
        tkey = (kind, key)
        old = self._buffer_tasks.get(tkey)
        if old:
            old.cancel()

        async def cleanup():
            await asyncio.sleep(SL_UNMATCHED_BUFFER_TTL_MS / 1000.0)
            buf = (self.state.recent_unmatched_sl if kind == "sl"
                   else self.state.recent_unmatched_tp)
            if buf.pop(key, None) is not None and kind == "sl":
                log.info(f"[{self.state.account_name}] Buffered SL for {key} "
                         f"expired without matching fill")

        self._buffer_tasks[tkey] = asyncio.create_task(cleanup())

    async def cleanup(self):
        for t in list(self._grace_tasks.values()):
            t.cancel()
        for t in list(self._buffer_tasks.values()):
            t.cancel()
        self._grace_tasks.clear()
        self._buffer_tasks.clear()
