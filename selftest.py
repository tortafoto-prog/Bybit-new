"""Offline end-to-end smoke test (no network). Run: python selftest.py

Exercises the real engine, SL tracker and sheets sync against an in-memory fake
Sheets client to verify the full write path and hedge-mode close matching.
"""

import asyncio

from constants import PositionMode, TradeStatus, ExitType
from models import AccountState
from sl_tracker import SLTracker
from trade_engine import TradeEngine
from sheets_sync import SheetsSync
from fills import parse_fill_ws
from handlers import OrderHandler


class FakeClient:
    def __init__(self):
        self.rows = []          # appended rows
        self.updates = {}       # row_number -> row

    async def append_row(self, values):
        self.rows.append(values)
        return len(self.rows) + 1   # pretend header is row 1

    async def update_row(self, row_number, values):
        self.updates[row_number] = values

    async def get_all_rows(self):
        return []


def fill(side, qty, price, closed=0.0, idx=0, exec_id="e", order_id="o",
         t=1_000, stop=""):
    return {
        "execId": exec_id, "orderId": order_id, "symbol": "ETHUSDT",
        "side": side, "execPrice": str(price), "execQty": str(qty),
        "execFee": "0", "execTime": str(t), "execType": "Trade",
        "positionIdx": str(idx), "closedSize": str(closed),
        "orderType": "Market", "stopOrderType": stop,
    }


async def run():
    failures = []

    def check(name, cond):
        print(("PASS" if cond else "FAIL"), name)
        if not cond:
            failures.append(name)

    # ── Scenario: hedge SHORT, open → SL set → close (the previously broken path)
    client = FakeClient()
    sheets = SheetsSync(client)
    await sheets.start()
    state = AccountState("Béla teszt", PositionMode.HEDGE)
    sl = SLTracker(state, sheets)
    engine = TradeEngine(state, sl, sheets)
    order_handler = OrderHandler("Béla teszt", sl, PositionMode.HEDGE)

    # open SHORT (Sell, closed=0) — WS omits positionIdx → should infer 2
    f_open = parse_fill_ws(fill("Sell", 0.22, 1961.88, closed=0.0, idx=0,
                                exec_id="x1", order_id="ord1"), PositionMode.HEDGE)
    check("open fill inferred idx=2 (hedge short)", f_open.position_idx == 2)
    await engine.on_open_fill(f_open)
    trade_id = next(iter(state.open_trades))
    check("trade created", len(state.open_trades) == 1)
    check("trade is PENDING", state.open_trades[trade_id].status == TradeStatus.PENDING)

    # SL order arrives (order side Buy → protects Sell/short position, idx 2)
    await order_handler.handle([{
        "stopOrderType": "StopLoss", "side": "Buy", "symbol": "ETHUSDT",
        "positionIdx": "0", "triggerPrice": "1966.52", "updatedTime": "1100",
        "orderId": "slord",
    }])
    tr = state.open_trades[trade_id]
    check("SL assigned", tr.sl_price == 1966.52)
    check("trade ACTIVE after SL", tr.status == TradeStatus.ACTIVE)

    # close SHORT: Buy fill with closed>0 → should infer idx=2 and match the trade
    f_close = parse_fill_ws(fill("Buy", 0.22, 1955.0, closed=0.22, idx=0,
                                 exec_id="x2", order_id="ord2", t=2000),
                            PositionMode.HEDGE)
    check("close fill inferred idx=2 (closes short)", f_close.position_idx == 2)
    await engine.on_close_fill(f_close)
    check("trade CLOSED", tr.status == TradeStatus.CLOSED)
    check("PnL computed", tr.pnl is not None and tr.pnl > 0)
    check("RR computed", tr.rr_ratio is not None)

    # let the flush loop write
    await asyncio.sleep(0.1)
    await sheets.stop()

    has_open = any(r[9] == "No" and r[11] == trade_id for r in client.rows) or \
        any(r[9] == "No" for r in client.updates.values())
    has_close = any(r[9] == "Yes" and r[11] == trade_id for r in client.rows)
    check("OPEN row written", has_open)
    check("CLOSE row written", has_close)

    close_row = next(r for r in client.rows if r[9] == "Yes")
    check("CLOSE direction inverted (BUY)", close_row[3] == "BUY")
    check("CLOSE comment has PnL", "PnL=" in close_row[13])
    check("Platform=Bybit", close_row[14] == "Bybit")

    # ── Scenario: one-way long, no SL within grace → INVALID flagged in sheet
    client2 = FakeClient()
    sheets2 = SheetsSync(client2)
    await sheets2.start()
    state2 = AccountState("Capri11", PositionMode.ONE_WAY)
    sl2 = SLTracker(state2, sheets2)
    engine2 = TradeEngine(state2, sl2, sheets2)
    import constants
    constants.SL_GRACE_PERIOD_MS = 50  # speed up
    f = parse_fill_ws(fill("Buy", 1.0, 2000.0, idx=0, exec_id="y1", order_id="oy1"),
                      PositionMode.ONE_WAY)
    f.exec_time_ms = 0  # force grace already in the past relative to wall clock... use timer
    await engine2.on_open_fill(parse_fill_ws(
        fill("Buy", 1.0, 2000.0, idx=0, exec_id="y1", order_id="oy1"),
        PositionMode.ONE_WAY))
    tid2 = next(iter(state2.open_trades))
    state2.open_trades[tid2].grace_deadline_ms = 1  # already expired
    sl2.start_grace_timer(state2.open_trades[tid2])
    await asyncio.sleep(0.1)
    check("no-SL trade marked INVALID", state2.open_trades[tid2].status == TradeStatus.INVALID)
    await sheets2.stop()
    open_row = next((r for r in client2.rows if r[9] == "No"), None)
    check("INVALID open row has HIBA note",
          open_row is not None and "HIBA! Nincs SL" in open_row[13])

    print("\n" + ("ALL PASSED" if not failures else f"FAILURES: {failures}"))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
