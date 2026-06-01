"""Startup / reconnect recovery: rebuild trades from REST history.

On startup (and after a WS gap) we fetch the execution and order history for the
recovery window and replay it deterministically:

  1. open fills (chronological)  → create PENDING trades
  2. match SL/TP to those trades  → parentOrderLinkId, then embedded SL/TP on the
                                     entry order, then latest SL/TP per position
  3. close fills (chronological) → FIFO close, RR/PnL, CLOSE rows
  4. backfill SL from SL_HIT fills; flag modified-SL (API can't return originals)
  5. resolve leftover PENDING → INVALID; validate against live positions
"""

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from models import AccountState, Fill, PositionKey, PositionSnapshot
from constants import (
    TradeStatus, ExitType, PositionMode,
    RECOVERY_MAX_WINDOW_DAYS, RECOVERY_PAGE_WINDOW_DAYS, QTY_TOLERANCE,
)
from fills import parse_fill_rest

if TYPE_CHECKING:
    from bybit_rest import BybitREST
    from trade_engine import TradeEngine
    from sl_tracker import SLTracker

log = logging.getLogger(__name__)
MS_PER_DAY = 86_400_000


@dataclass
class _Cond:
    """A conditional SL or TP order (or an SL/TP embedded on an entry order)."""
    order_id: str
    symbol: str
    position_side: str   # side of the POSITION it protects
    position_idx: int
    trigger: float
    created_ms: int
    updated_ms: int
    source_order_id: str = ""   # entry order id (for embedded SL/TP)
    parent_link_id: str = ""    # entry order's orderLinkId (Bybit auto-link)
    status: str = ""


def _pos_side_from_order(order_side: str) -> Optional[str]:
    """SL/TP order side is opposite to the position side."""
    if order_side == "Sell":
        return "Buy"
    if order_side == "Buy":
        return "Sell"
    return None


class RecoveryEngine:
    def __init__(self, state: AccountState, rest: "BybitREST",
                 engine: "TradeEngine", sl_tracker: "SLTracker", mode: PositionMode):
        self.state = state
        self.rest = rest
        self.engine = engine
        self.sl = sl_tracker
        self.mode = mode

    async def recover(self, gap_start_ms: Optional[int] = None,
                      gap_end_ms: Optional[int] = None, force_full: bool = False):
        gap_end_ms = gap_end_ms or int(time.time() * 1000)
        max_gap = RECOVERY_MAX_WINDOW_DAYS * MS_PER_DAY
        if force_full or gap_start_ms is None:
            gap_start_ms = gap_end_ms - max_gap
            log.info(f"[{self.state.account_name}] Full recovery: last "
                     f"{RECOVERY_MAX_WINDOW_DAYS} day(s)")
        elif gap_end_ms - gap_start_ms > max_gap:
            gap_start_ms = gap_end_ms - max_gap
        if gap_end_ms - gap_start_ms < 1000:
            return

        # 1. fetch ──────────────────────────────────────────────────────────
        raw_fills, raw_orders = [], []
        w = gap_start_ms
        while w < gap_end_ms:
            we = min(w + RECOVERY_PAGE_WINDOW_DAYS * MS_PER_DAY, gap_end_ms)
            raw_fills.extend(await self.rest.get_all_executions(w, we))
            for of in (None, "StopOrder", "tpslOrder"):
                raw_orders.extend(await self.rest.get_all_order_history(of, w, we))
            w = we

        fills = []
        for r in raw_fills:
            try:
                f = parse_fill_rest(r, self.mode)
                if f.exec_type in ("Trade", "BustTrade") and f.exec_id not in self.state.seen_exec_ids:
                    fills.append(f)
            except Exception as e:
                log.error(f"[{self.state.account_name}] Fill parse error: {e}")

        conds, link_to_id = self._collect_conditionals(raw_orders)
        log.info(f"[{self.state.account_name}] Fetched {len(fills)} new fills, "
                 f"{len(conds)} SL/TP orders")

        if not fills:
            await self.validate_current_positions()
            return

        fills.sort(key=lambda f: f.exec_time_ms)
        open_fills = [f for f in fills if f.is_open_fill]
        close_fills = [f for f in fills if f.is_close_fill]

        # 2. open fills → trades ────────────────────────────────────────────
        for f in open_fills:
            try:
                await self.engine.on_open_fill(f)
            except Exception as e:
                log.error(f"[{self.state.account_name}] Open fill error: {e}", exc_info=True)

        # 3. match SL/TP to the freshly-created trades ──────────────────────
        self._match_conditionals(conds, link_to_id)

        # 4. close fills ────────────────────────────────────────────────────
        for f in close_fills:
            try:
                await self.engine.on_close_fill(f)
            except Exception as e:
                log.error(f"[{self.state.account_name}] Close fill error: {e}", exc_info=True)

        # 5. backfill SL from SL_HIT fills ──────────────────────────────────
        self._backfill_sl_from_fills(close_fills, conds)

        # 6. resolve leftover PENDING + validate live ───────────────────────
        await self._resolve_pending()
        await self.validate_current_positions()
        log.info(f"[{self.state.account_name}] Recovery complete")

    # ── conditional collection ──────────────────────────────────────────────
    def _collect_conditionals(self, raw_orders: list[dict]) -> tuple[list[_Cond], dict]:
        conds: list[_Cond] = []
        link_to_id: dict[str, str] = {}
        seen: set[str] = set()
        for r in raw_orders:
            olk, oid = r.get("orderLinkId", ""), r.get("orderId", "")
            if olk and oid:
                link_to_id[olk] = oid

        for r in raw_orders:
            oid = r.get("orderId", "")
            if oid in seen:
                continue
            seen.add(oid)
            symbol = r.get("symbol", "")
            idx = int(r.get("positionIdx", "0"))
            created = int(r.get("createdTime", "0") or 0)
            updated = int(r.get("updatedTime", r.get("createdTime", "0")) or 0)
            stop_type = r.get("stopOrderType", "")
            order_side = r.get("side", "")
            trigger = float(r.get("triggerPrice", "0") or 0)

            if stop_type == "StopLoss" and trigger > 0:
                ps = _pos_side_from_order(order_side)
                if ps:
                    conds.append(_Cond(oid, symbol, ps, idx, trigger, created, updated,
                                       parent_link_id=r.get("parentOrderLinkId", ""),
                                       status=r.get("orderStatus", "")))
            # embedded SL on entry order (tpslMode=Full)
            emb_sl = r.get("stopLoss", "")
            if emb_sl and float(emb_sl) > 0 and stop_type != "StopLoss":
                conds.append(_Cond(oid + "_emb_sl", symbol, order_side, idx,
                                   float(emb_sl), created, updated, source_order_id=oid))
        return conds, link_to_id

    def _match_conditionals(self, conds: list[_Cond], link_to_id: dict):
        sl_conds = [c for c in conds]
        for trade in sorted(self.state.open_trades.values(), key=lambda t: t.entry_time_ms):
            if trade.sl_price is not None:
                continue
            match = self._best_sl_for(trade, sl_conds, link_to_id)
            if not match:
                continue
            on_side = ((trade.side == "Buy" and match.trigger < trade.entry_price) or
                       (trade.side == "Sell" and match.trigger > trade.entry_price))
            if not on_side:
                trade.sl_source = "modified_sl_api_limit"
                continue
            trade.sl_price = match.trigger
            trade.sl_assigned_time_ms = match.updated_ms
            trade.sl_source = "rest_recovery"
            if trade.status == TradeStatus.PENDING:
                trade.status = TradeStatus.ACTIVE
            self._remove_pending(trade)

    def _best_sl_for(self, trade, conds: list[_Cond], link_to_id: dict) -> Optional[_Cond]:
        same = [c for c in conds
                if c.symbol == trade.symbol
                and c.position_side == trade.side
                and (c.position_idx == trade.position_idx or trade.position_idx == 0 or c.position_idx == 0)]
        # 1. embedded on this exact entry order
        for c in same:
            if c.source_order_id and c.source_order_id == trade.entry_order_id:
                return c
        # 2. parentOrderLinkId → entry order id
        for c in same:
            if c.parent_link_id and link_to_id.get(c.parent_link_id) == trade.entry_order_id:
                return c
        # 3. latest SL created at/after entry
        after = [c for c in same if c.created_ms >= trade.entry_time_ms - 5000]
        if after:
            return max(after, key=lambda c: c.updated_ms)
        return None

    def _backfill_sl_from_fills(self, close_fills: list[Fill], conds: list[_Cond]):
        by_id = {c.order_id: c for c in conds}
        for trade in self.state.open_trades.values():
            if trade.sl_price is not None or trade.exit_type != ExitType.SL_HIT:
                continue
            for eid in trade.exit_exec_ids:
                for f in close_fills:
                    if f.exec_id == eid and f.stop_order_type == "StopLoss":
                        c = by_id.get(f.order_id)
                        if not c:
                            continue
                        on_side = ((trade.side == "Buy" and c.trigger < trade.entry_price) or
                                   (trade.side == "Sell" and c.trigger > trade.entry_price))
                        if on_side:
                            trade.sl_price = c.trigger
                            trade.sl_source = "rest_backfill_from_fill"
                        else:
                            trade.sl_source = "modified_sl_api_limit"
                        break

    # ── pending / live ──────────────────────────────────────────────────────
    async def _resolve_pending(self):
        now = int(time.time() * 1000)
        for trade in list(self.state.open_trades.values()):
            if trade.status != TradeStatus.PENDING:
                continue
            if trade.grace_deadline_ms and now > trade.grace_deadline_ms:
                trade.status = TradeStatus.INVALID
                trade.updated_at_ms = now
                self._remove_pending(trade)
                await self.engine.sheets.enqueue_update(trade)
            else:
                self.sl.start_grace_timer(trade)

    async def validate_current_positions(self):
        try:
            positions = await self.rest.get_positions()
        except Exception as e:
            log.error(f"[{self.state.account_name}] Position validation error: {e}")
            return
        for raw in positions:
            size = float(raw.get("size", "0") or 0)
            if size <= 0:
                continue
            idx = int(raw.get("positionIdx", "0"))
            key = PositionKey(raw["symbol"], idx)

            def f(v, d=0.0):
                return float(v) if v not in (None, "") else d

            snap = PositionSnapshot(
                key=key, side=raw.get("side", ""), size=size,
                entry_price=f(raw.get("avgPrice")), sl_price=f(raw.get("stopLoss")),
                tp_price=f(raw.get("takeProfit")), leverage=f(raw.get("leverage"), 1.0),
                liq_price=f(raw.get("liqPrice")), updated_at_ms=int(raw.get("updatedTime", "0") or 0))
            self.state.positions[key] = snap
            if snap.sl_price > 0:
                self.state.last_known_sl[key] = snap.sl_price
            if snap.tp_price > 0:
                self.state.last_known_tp[key] = snap.tp_price

    def _remove_pending(self, trade):
        for q in (self.state.pending_sl_queue, self.state.pending_tp_queue):
            lst = q.get(trade.pos_key)
            if lst and trade.trade_id in lst:
                lst.remove(trade.trade_id)
                if not lst:
                    q.pop(trade.pos_key, None)
