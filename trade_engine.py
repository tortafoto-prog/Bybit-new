"""Trade lifecycle: open fills create/extend trades, close fills match FIFO.

One entry order == one internal trade. Partial fills of the same order_id are
aggregated (volume-weighted). Exit fills are matched against open trades of the
same symbol+positionIdx in FIFO order, computing RR and PnL on full close.
"""

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from models import AccountState, Fill, InternalTrade
from constants import TradeStatus, ExitType, SL_GRACE_PERIOD_MS, QTY_TOLERANCE

if TYPE_CHECKING:
    from sl_tracker import SLTracker
    from sheets_sync import SheetsSync

log = logging.getLogger(__name__)


def _invert(side: str) -> str:
    return "Sell" if side == "Buy" else "Buy"


def _exit_type(fill: Fill) -> ExitType:
    if fill.exec_type == "BustTrade":
        return ExitType.LIQUIDATION
    if fill.stop_order_type == "StopLoss":
        return ExitType.SL_HIT
    if fill.stop_order_type == "TakeProfit":
        return ExitType.TP_HIT
    return ExitType.MANUAL


class TradeEngine:
    def __init__(self, state: AccountState, sl_tracker: "SLTracker", sheets: "SheetsSync"):
        self.state = state
        self.sl = sl_tracker
        self.sheets = sheets
        self._order_to_trade: dict[str, str] = {}

    # ── open ────────────────────────────────────────────────────────────────
    async def on_open_fill(self, fill: Fill):
        if fill.exec_id in self.state.seen_exec_ids:
            return
        self.state.seen_exec_ids.add(fill.exec_id)
        self.state.last_exec_time_ms = max(self.state.last_exec_time_ms, fill.exec_time_ms)

        now = int(time.time() * 1000)
        open_qty = fill.open_qty

        existing = self._order_to_trade.get(fill.order_id)
        if existing and existing in self.state.open_trades:
            trade = self.state.open_trades[existing]
            total = trade.entry_price * trade.entry_qty + fill.exec_price * open_qty
            trade.entry_qty += open_qty
            trade.entry_price = total / trade.entry_qty
            trade.entry_fee += fill.exec_fee * (open_qty / fill.exec_qty) if fill.exec_qty else 0
            trade.grace_deadline_ms = fill.exec_time_ms + SL_GRACE_PERIOD_MS
            trade.updated_at_ms = now
            await self.sheets.enqueue_update(trade)
            return

        trade_id = f"{self.state.account_name}_{fill.symbol}_{fill.side}_{fill.order_id[:12]}"
        trade = InternalTrade(
            trade_id=trade_id,
            account_name=self.state.account_name,
            symbol=fill.symbol,
            side=fill.side,
            position_idx=fill.position_idx,
            entry_price=fill.exec_price,
            entry_qty=open_qty,
            entry_time_ms=fill.exec_time_ms,
            entry_exec_id=fill.exec_id,
            entry_order_id=fill.order_id,
            entry_fee=fill.exec_fee * (open_qty / fill.exec_qty) if fill.exec_qty else 0,
            entry_order_type=(fill.stop_order_type or fill.order_type or "Market"),
            status=TradeStatus.PENDING,
            grace_deadline_ms=fill.exec_time_ms + SL_GRACE_PERIOD_MS,
            created_at_ms=now,
            updated_at_ms=now,
        )
        self.state.open_trades[trade_id] = trade
        self._order_to_trade[fill.order_id] = trade_id
        log.info(f"[{self.state.account_name}] New trade {trade_id}: "
                 f"{fill.side} {open_qty} {fill.symbol} @ {fill.exec_price}")
        self.sl.register_pending_trade(trade)
        await self.sheets.enqueue_open(trade)

    # ── close ───────────────────────────────────────────────────────────────
    async def on_close_fill(self, fill: Fill):
        if fill.exec_id in self.state.seen_exec_ids:
            return
        self.state.seen_exec_ids.add(fill.exec_id)
        self.state.last_exec_time_ms = max(self.state.last_exec_time_ms, fill.exec_time_ms)

        exit_type = _exit_type(fill)
        position_side = _invert(fill.side)

        candidates = [
            t for t in self.state.open_trades.values()
            if t.symbol == fill.symbol
            and t.position_idx == fill.position_idx
            and t.side == position_side
            and t.status in (TradeStatus.ACTIVE, TradeStatus.PENDING, TradeStatus.INVALID)
            and t.remaining_qty > QTY_TOLERANCE
        ]
        candidates.sort(key=lambda t: t.entry_time_ms)

        remaining = fill.closed_size
        for trade in candidates:
            if remaining <= QTY_TOLERANCE:
                break
            portion = min(remaining, trade.remaining_qty)
            if abs(portion - trade.remaining_qty) < QTY_TOLERANCE:
                portion = trade.remaining_qty

            if trade.exit_price is None:
                trade.exit_price = fill.exec_price
            else:
                prev = trade.exit_price * trade.exit_qty
                trade.exit_price = (prev + fill.exec_price * portion) / (trade.exit_qty + portion)
            trade.exit_qty += portion
            trade.exit_time_ms = fill.exec_time_ms
            trade.exit_exec_ids.append(fill.exec_id)
            trade.exit_fee += fill.exec_fee * (portion / fill.closed_size) if fill.closed_size else 0
            trade.exit_type = exit_type
            trade.updated_at_ms = int(time.time() * 1000)
            remaining -= portion

            if abs(trade.exit_qty - trade.entry_qty) < QTY_TOLERANCE:
                trade.exit_qty = trade.entry_qty
                self._finalize(trade)
                await self.sheets.enqueue_close(trade)
                self._schedule_removal(trade.trade_id)
            else:
                log.info(f"[{self.state.account_name}] Partial close {trade.trade_id}: "
                         f"{portion}, remaining {trade.remaining_qty}")
                await self.sheets.enqueue_update(trade)

        if remaining > QTY_TOLERANCE:
            log.warning(f"[{self.state.account_name}] Unmatched exit qty {remaining} "
                        f"{fill.symbol} idx={fill.position_idx} (exec {fill.exec_id})")

    def _finalize(self, trade: InternalTrade):
        trade.status = TradeStatus.CLOSED
        trade.rr_ratio = trade.calculate_rr()
        trade.pnl = trade.calculate_pnl()
        log.info(f"[{self.state.account_name}] CLOSED {trade.trade_id} "
                 f"entry={trade.entry_price} exit={trade.exit_price} "
                 f"RR={trade.rr_ratio} PnL={trade.pnl} "
                 f"{trade.exit_type.value if trade.exit_type else '?'}")

    def _schedule_removal(self, trade_id: str, delay_s: float = 300):
        async def remove():
            await asyncio.sleep(delay_s)
            self.state.open_trades.pop(trade_id, None)
        asyncio.create_task(remove())
