"""WebSocket message dispatch + topic handlers (execution / order / position)."""

import logging
from typing import TYPE_CHECKING

from models import SLEvent, TPEvent, PositionSnapshot, PositionKey
from constants import PositionMode
from fills import parse_fill_ws

if TYPE_CHECKING:
    from trade_engine import TradeEngine
    from sl_tracker import SLTracker
    from models import AccountState

log = logging.getLogger(__name__)


class Dispatcher:
    def __init__(self, account_name: str):
        self.account_name = account_name
        self._handlers = {}

    def register(self, topic: str, handler):
        self._handlers[topic] = handler

    async def dispatch(self, message: dict):
        topic = message.get("topic", "")
        data = message.get("data")
        if not topic or data is None:
            return
        handler = self._handlers.get(topic)
        if handler is None:
            return
        try:
            await handler(data)
        except Exception as e:
            log.error(f"[{self.account_name}] Handler error topic={topic}: {e}", exc_info=True)


class ExecutionHandler:
    def __init__(self, account_name: str, engine: "TradeEngine", mode: PositionMode):
        self.account_name = account_name
        self.engine = engine
        self.mode = mode

    async def handle(self, data: list[dict]):
        for raw in data:
            try:
                fill = parse_fill_ws(raw, self.mode)
                if fill.exec_type not in ("Trade", "BustTrade"):
                    continue
                log.info(f"[{self.account_name}] Fill: {fill.side} {fill.exec_qty} "
                         f"{fill.symbol} @ {fill.exec_price} "
                         f"(closed={fill.closed_size}, idx={fill.position_idx})")
                if fill.is_close_fill:
                    await self.engine.on_close_fill(fill)
                if fill.is_open_fill:
                    await self.engine.on_open_fill(fill)
            except Exception as e:
                log.error(f"[{self.account_name}] Execution error: {e}", exc_info=True)


class OrderHandler:
    """Detects SL/TP conditional order changes (primary SL source)."""

    def __init__(self, account_name: str, sl_tracker: "SLTracker", mode: PositionMode):
        self.account_name = account_name
        self.sl = sl_tracker
        self.mode = mode

    async def handle(self, data: list[dict]):
        for raw in data:
            try:
                sot = raw.get("stopOrderType", "")
                if sot not in ("StopLoss", "TakeProfit"):
                    continue
                order_side = raw.get("side", "")
                # SL/TP order side is opposite to the position it protects.
                if order_side == "Sell":
                    position_side, hedge_idx = "Buy", 1
                elif order_side == "Buy":
                    position_side, hedge_idx = "Sell", 2
                else:
                    continue
                raw_idx = int(raw.get("positionIdx", "0"))
                if raw_idx in (1, 2):
                    position_idx = raw_idx
                elif self.mode == PositionMode.HEDGE:
                    position_idx = hedge_idx
                else:
                    position_idx = 0
                trigger = float(raw.get("triggerPrice", "0") or 0)
                ts = int(raw.get("updatedTime", "0") or 0)
                oid = raw.get("orderId", "")

                if sot == "StopLoss":
                    log.info(f"[{self.account_name}] SL order: {raw.get('symbol')} "
                             f"pos={position_side} SL={trigger} id={oid}")
                    await self.sl.on_sl_change(SLEvent(
                        symbol=raw["symbol"], side=position_side, position_idx=position_idx,
                        sl_price=trigger if trigger > 0 else 0.0, sl_order_id=oid,
                        timestamp_ms=ts, source="order_ws"))
                else:
                    log.info(f"[{self.account_name}] TP order: {raw.get('symbol')} "
                             f"pos={position_side} TP={trigger} id={oid}")
                    await self.sl.on_tp_change(TPEvent(
                        symbol=raw["symbol"], side=position_side, position_idx=position_idx,
                        tp_price=trigger if trigger > 0 else 0.0, tp_order_id=oid,
                        timestamp_ms=ts, source="order_ws"))
            except Exception as e:
                log.error(f"[{self.account_name}] Order error: {e}", exc_info=True)


class PositionHandler:
    """Secondary SL/TP source + position snapshot tracking."""

    def __init__(self, account_name: str, state: "AccountState", sl_tracker: "SLTracker"):
        self.account_name = account_name
        self.state = state
        self.sl = sl_tracker

    async def handle(self, data: list[dict]):
        for raw in data:
            try:
                def f(v, d=0.0):
                    return float(v) if v not in (None, "") else d

                symbol = raw["symbol"]
                idx = int(raw.get("positionIdx", "0"))
                key = PositionKey(symbol, idx)
                size = f(raw.get("size"))
                sl = f(raw.get("stopLoss"))
                tp = f(raw.get("takeProfit"))
                side = raw.get("side", "")
                ts = int(raw.get("updatedTime", "0") or 0)

                self.state.positions[key] = PositionSnapshot(
                    key=key, side=side, size=size, entry_price=f(raw.get("entryPrice")),
                    sl_price=sl, tp_price=tp, leverage=f(raw.get("leverage"), 1.0),
                    liq_price=f(raw.get("liqPrice")), updated_at_ms=ts)

                if sl != self.state.last_known_sl.get(key, 0.0):
                    log.info(f"[{self.account_name}] Position SL change {symbol}: "
                             f"{self.state.last_known_sl.get(key, 0.0)} → {sl}")
                    await self.sl.on_sl_change(SLEvent(
                        symbol=symbol, side=side, position_idx=idx, sl_price=sl,
                        sl_order_id=None, timestamp_ms=ts, source="position_ws"))

                if tp != self.state.last_known_tp.get(key, 0.0):
                    await self.sl.on_tp_change(TPEvent(
                        symbol=symbol, side=side, position_idx=idx, tp_price=tp,
                        tp_order_id=None, timestamp_ms=ts, source="position_ws"))

                if size == 0:
                    log.info(f"[{self.account_name}] Position closed: {symbol}")
            except Exception as e:
                log.error(f"[{self.account_name}] Position error: {e}", exc_info=True)
