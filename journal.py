"""Journal: turns InternalTrades into doPost payloads and posts them.

Because the doPost web app can only *append* (and dedups by Ticket ID), each
trade is posted at most once as OPEN and once as CLOSE:

  - OPEN  is posted when the SL status is final — i.e. when the trade becomes
          ACTIVE (SL assigned) or INVALID (grace expired). A no-SL OPEN is still
          posted; the web app journals it with a "HIBA! Nincs SL" comment.
  - CLOSE is posted on full close (and will lazily post OPEN first if a trade
          opened and closed before its SL resolved).

Posting runs on a background queue so the engine never blocks on the network.
"""

import asyncio
import logging

from models import InternalTrade
from constants import ExitType
from util import ts_str
from dopost_client import DoPostClient

log = logging.getLogger(__name__)


class Journal:
    def __init__(self, client: DoPostClient):
        self.client = client
        self._q: asyncio.Queue = asyncio.Queue()
        self._running = False

    async def start(self):
        self._running = True
        asyncio.create_task(self._loop())

    async def stop(self):
        self._running = False
        await self._drain()

    # ── public API ──────────────────────────────────────────────────────────
    async def post_open(self, trade: InternalTrade):
        if trade.open_posted:
            return
        trade.open_posted = True
        await self._q.put(("open", trade))

    async def post_close(self, trade: InternalTrade):
        # Make sure the OPEN exists first (fast scalps may close pre-SL).
        if not trade.open_posted:
            trade.open_posted = True
            await self._q.put(("open", trade))
        if trade.close_posted:
            return
        trade.close_posted = True
        await self._q.put(("close", trade))

    # ── background loop ─────────────────────────────────────────────────────
    async def _loop(self):
        while self._running:
            try:
                kind, trade = await asyncio.wait_for(self._q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            await self._send(kind, trade)

    async def _drain(self):
        while not self._q.empty():
            kind, trade = self._q.get_nowait()
            await self._send(kind, trade)

    async def _send(self, kind: str, trade: InternalTrade):
        payload = _open_payload(trade) if kind == "open" else _close_payload(trade)
        resp = await self.client.post(payload)
        status = resp.get("status", "?")
        sync = resp.get("sync") or resp.get("message") or ""
        log.info(f"doPost {kind.upper()} {trade.trade_id}: status={status} {sync}".rstrip())
        if status in ("error", "Unauthorized"):
            log.error(f"doPost {kind} rejected for {trade.trade_id}: {resp}")


def _open_payload(t: InternalTrade) -> dict:
    return {
        "source": "bybit",
        "account_id": t.account_name,
        "symbol": t.symbol,
        "direction": t.side.upper(),                  # BUY / SELL
        "price": t.entry_price,
        "volume": t.entry_qty,
        "stop_loss": t.sl_price if t.sl_price is not None else 0,
        "take_profit": t.tp_price if t.tp_price is not None else 0,
        "ticket_id": t.entry_exec_id,
        "is_closing": "No",
        "order_type": (t.entry_order_type or "Market").upper(),
        "position_id": t.trade_id,
        "open_time": ts_str(t.entry_time_ms),
        "comment": "",                                # web app adds HIBA if no SL
    }


def _close_payload(t: InternalTrade) -> dict:
    order_type = "MARKET"
    if t.exit_type in (ExitType.SL_HIT, ExitType.TP_HIT):
        order_type = "STOP_LIMIT"
    elif t.exit_type == ExitType.LIQUIDATION:
        order_type = "LIQUIDATION"

    parts = []
    if t.exit_type:
        parts.append(t.exit_type.value)
    if t.rr_ratio is not None:
        parts.append(f"RR={t.rr_ratio}")
    if t.pnl is not None:
        parts.append(f"PnL={t.pnl}")
    comment = " | ".join(parts)

    return {
        "source": "bybit",
        "account_id": t.account_name,
        "symbol": t.symbol,
        "direction": "SELL" if t.side == "Buy" else "BUY",   # inverted on close
        "price": t.exit_price,
        "volume": t.exit_qty,
        "stop_loss": 0,
        "take_profit": 0,
        "ticket_id": ",".join(t.exit_exec_ids),
        "is_closing": "Yes",
        "order_type": order_type,
        "position_id": t.trade_id,
        "open_time": ts_str(t.exit_time_ms),
        "comment": comment,
    }
