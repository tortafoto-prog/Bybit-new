"""Offline end-to-end smoke test (no network). Run: python selftest.py

Exercises the real engine, SL tracker and journal against a fake doPost client
that records the posted payloads — verifying the full flow and that OPEN is
posted only when the SL status is settled.
"""

import asyncio
import time

from constants import PositionMode, TradeStatus, ExitType
from models import AccountState
from sl_tracker import SLTracker
from trade_engine import TradeEngine
from journal import Journal
from fills import parse_fill_ws
from handlers import OrderHandler
import constants


class FakeDoPost:
    def __init__(self):
        self.posts = []  # list of payloads

    async def post(self, payload):
        self.posts.append(payload)
        return {"status": "success", "sync": "ok"}


_NOW = int(time.time() * 1000)


def fill(side, qty, price, closed=0.0, idx=0, exec_id="e", order_id="o", t=None, stop=""):
    # Realistic epoch-ms exec time so the grace deadline lands ~now+60s.
    t = _NOW if t is None else t
    return {"execId": exec_id, "orderId": order_id, "symbol": "ETHUSDT", "side": side,
            "execPrice": str(price), "execQty": str(qty), "execFee": "0",
            "execTime": str(t), "execType": "Trade", "positionIdx": str(idx),
            "closedSize": str(closed), "orderType": "Market", "stopOrderType": stop}


async def settle():
    await asyncio.sleep(0.05)


async def run():
    failures = []

    def check(name, cond):
        print(("PASS" if cond else "FAIL"), name)
        if not cond:
            failures.append(name)

    # ── Scenario A: hedge SHORT, open → SL → close ──────────────────────────
    fake = FakeDoPost()
    journal = Journal(fake)
    await journal.start()
    state = AccountState("Béla teszt", PositionMode.HEDGE)
    sl = SLTracker(state, journal)
    engine = TradeEngine(state, sl, journal)
    orders = OrderHandler("Béla teszt", sl, PositionMode.HEDGE)

    f_open = parse_fill_ws(fill("Sell", 0.22, 1961.88, idx=0, exec_id="x1", order_id="o1"),
                           PositionMode.HEDGE)
    check("open fill idx=2 (hedge short)", f_open.position_idx == 2)
    await engine.on_open_fill(f_open)
    await settle()
    check("no OPEN posted before SL", len(fake.posts) == 0)

    await orders.handle([{"stopOrderType": "StopLoss", "side": "Buy", "symbol": "ETHUSDT",
                          "positionIdx": "0", "triggerPrice": "1966.52",
                          "updatedTime": "1100", "orderId": "sl1"}])
    await settle()
    open_posts = [p for p in fake.posts if p["is_closing"] == "No"]
    check("OPEN posted after SL", len(open_posts) == 1)
    check("OPEN has SL", open_posts and open_posts[0]["stop_loss"] == 1966.52)
    check("OPEN direction SELL", open_posts and open_posts[0]["direction"] == "SELL")
    check("OPEN source bybit", open_posts and open_posts[0]["source"] == "bybit")

    tid = next(iter(state.open_trades))
    f_close = parse_fill_ws(fill("Buy", 0.22, 1955.0, closed=0.22, idx=0,
                                 exec_id="x2", order_id="o2", t=_NOW + 1000), PositionMode.HEDGE)
    check("close fill idx=2 (closes short)", f_close.position_idx == 2)
    await engine.on_close_fill(f_close)
    await settle()
    close_posts = [p for p in fake.posts if p["is_closing"] == "Yes"]
    check("CLOSE posted", len(close_posts) == 1)
    check("CLOSE direction inverted BUY", close_posts and close_posts[0]["direction"] == "BUY")
    check("CLOSE has PnL in comment", close_posts and "PnL=" in close_posts[0]["comment"])
    check("trade CLOSED", state.open_trades[tid].status == TradeStatus.CLOSED)
    check("exactly one OPEN + one CLOSE", len(fake.posts) == 2)
    await journal.stop()

    # ── Scenario B: no SL within grace → INVALID open posted with SL=0 ───────
    constants.SL_GRACE_PERIOD_MS = 50
    fake2 = FakeDoPost()
    journal2 = Journal(fake2)
    await journal2.start()
    state2 = AccountState("Capri11", PositionMode.ONE_WAY)
    sl2 = SLTracker(state2, journal2)
    engine2 = TradeEngine(state2, sl2, journal2)
    await engine2.on_open_fill(parse_fill_ws(
        fill("Buy", 1.0, 2000.0, idx=0, exec_id="y1", order_id="oy1"), PositionMode.ONE_WAY))
    tid2 = next(iter(state2.open_trades))
    state2.open_trades[tid2].grace_deadline_ms = 1  # already expired
    sl2.start_grace_timer(state2.open_trades[tid2])
    await asyncio.sleep(0.1)
    check("no-SL trade INVALID", state2.open_trades[tid2].status == TradeStatus.INVALID)
    inv = [p for p in fake2.posts if p["is_closing"] == "No"]
    check("INVALID OPEN posted", len(inv) == 1)
    check("INVALID OPEN stop_loss=0", inv and inv[0]["stop_loss"] == 0)
    await journal2.stop()

    print("\n" + ("ALL PASSED" if not failures else f"FAILURES: {failures}"))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
