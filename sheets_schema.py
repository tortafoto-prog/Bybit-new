"""Google Sheets row schema — 15 columns, two rows per trade (OPEN + CLOSE).

Matches the live "Trades" sheet exactly (verified against the production CSV
export). Column order A–O:

  A Timestamp     F Volume      K Order Type
  B Account ID    G Ticket ID   L Position ID
  C Symbol        H Stop Loss   M Open Time
  D Direction     I Take Profit N Comment
  E Price         J Is Closing  O Platform

Dates use the format `YYYY.MM.DD. HH:MM:SS`. Numbers are written as raw floats;
the sheet's own locale renders the decimal separator (the live sheet shows
commas). Direction is BUY/SELL; the CLOSE row uses the inverted direction.
"""

from datetime import datetime, timezone
from typing import Optional

from models import InternalTrade
from constants import TradeStatus, ExitType

HEADERS = [
    "Timestamp", "Account ID", "Symbol", "Direction", "Price", "Volume",
    "Ticket ID", "Stop Loss", "Take Profit", "Is Closing", "Order Type",
    "Position ID", "Open Time", "Comment", "Platform",
]

_DATE_FMT = "%Y.%m.%d. %H:%M:%S"


def _ts(ms: Optional[int]) -> str:
    if not ms or ms <= 0:
        return ""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(_DATE_FMT)


def _no_sl_note(trade: InternalTrade) -> str:
    if trade.sl_source == "modified_sl_api_limit":
        return "HIBA! Nincs SL (API korlát: módosított SL)"
    return "HIBA! Nincs SL"


def trade_to_open_row(trade: InternalTrade) -> list:
    comment = "" if trade.sl_price is not None else _no_sl_note(trade)
    order_type = (trade.entry_order_type or "Market").upper()
    return [
        _ts(trade.entry_time_ms),                               # A Timestamp
        trade.account_name,                                     # B Account ID
        trade.symbol,                                           # C Symbol
        trade.side.upper(),                                     # D Direction
        trade.entry_price,                                      # E Price
        trade.entry_qty,                                        # F Volume
        trade.entry_exec_id,                                    # G Ticket ID
        trade.sl_price if trade.sl_price is not None else "",   # H Stop Loss
        trade.tp_price if trade.tp_price is not None else "",   # I Take Profit
        "No",                                                   # J Is Closing
        order_type,                                             # K Order Type
        trade.trade_id,                                         # L Position ID
        _ts(trade.entry_time_ms),                               # M Open Time
        comment,                                                # N Comment
        "Bybit",                                                # O Platform
    ]


def trade_to_close_row(trade: InternalTrade) -> list:
    close_direction = "SELL" if trade.side == "Buy" else "BUY"
    exit_ticket = ",".join(trade.exit_exec_ids) if trade.exit_exec_ids else ""

    order_type = "MARKET"
    if trade.exit_type:
        if trade.exit_type in (ExitType.SL_HIT, ExitType.TP_HIT):
            order_type = "STOP_LIMIT"
        elif trade.exit_type == ExitType.LIQUIDATION:
            order_type = "LIQUIDATION"

    parts: list[str] = []
    if trade.exit_type:
        parts.append(trade.exit_type.value)
    if trade.rr_ratio is not None:
        parts.append(f"RR={trade.rr_ratio}")
    if trade.pnl is not None:
        parts.append(f"PnL={trade.pnl}")
    if trade.sl_price is None:
        parts.append(_no_sl_note(trade))
    comment = " | ".join(parts)

    return [
        _ts(trade.exit_time_ms),                                # A Timestamp
        trade.account_name,                                     # B Account ID
        trade.symbol,                                           # C Symbol
        close_direction,                                        # D Direction
        trade.exit_price if trade.exit_price else "",           # E Price
        trade.exit_qty if trade.exit_qty > 0 else "",           # F Volume
        exit_ticket,                                            # G Ticket ID
        "",                                                     # H Stop Loss
        "",                                                     # I Take Profit
        "Yes",                                                  # J Is Closing
        order_type,                                             # K Order Type
        trade.trade_id,                                         # L Position ID
        _ts(trade.exit_time_ms),                                # M Open Time
        comment,                                                # N Comment
        "Bybit",                                                # O Platform
    ]


# ── parsing back from sheet rows (startup state load) ───────────────────────
def _f(s: str) -> float:
    if not s:
        return 0.0
    return float(str(s).replace(",", ".").strip())


def _to_ms(s: str) -> int:
    if not s:
        return 0
    for fmt in (_DATE_FMT, "%Y-%m-%d %H:%M:%S"):
        try:
            return int(datetime.strptime(s.strip(), fmt)
                       .replace(tzinfo=timezone.utc).timestamp() * 1000)
        except ValueError:
            continue
    return 0


def open_row_to_trade(row: list, row_number: int, hedge: bool = False) -> InternalTrade:
    """Reconstruct a still-open trade from its OPEN sheet row.

    The sheet has no positionIdx column, so in hedge mode we infer it from the
    side (Buy→1 long, Sell→2 short); otherwise it is 0. Getting this right is
    essential — exit fills are matched by symbol+positionIdx, so a wrong idx
    leaves the close "unmatched" and the trade never gets a CLOSE row.
    """
    def g(i: int) -> str:
        return str(row[i]).strip() if i < len(row) and row[i] is not None else ""

    side = "Buy" if g(3).upper() == "BUY" else "Sell"
    if hedge:
        position_idx = 1 if side == "Buy" else 2
    else:
        position_idx = 0
    sl = _f(g(7))
    tp = _f(g(8))
    return InternalTrade(
        trade_id=g(11),
        account_name=g(1),
        symbol=g(2),
        side=side,
        position_idx=position_idx,
        entry_price=_f(g(4)),
        entry_qty=_f(g(5)),
        entry_time_ms=_to_ms(g(0)),
        entry_exec_id=g(6),
        entry_order_id="",
        sl_price=sl if sl > 0 else None,
        tp_price=tp if tp > 0 else None,
        status=TradeStatus.ACTIVE if sl > 0 else TradeStatus.PENDING,
        sheets_open_row=row_number,
    )
