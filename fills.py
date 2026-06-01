"""Fill parsing shared by the live WS handler and REST recovery.

Critically, Bybit's WS execution topic often omits `positionIdx` (defaults to 0)
even on hedge-mode accounts. We reconstruct it from the fill side and whether it
opens or closes a position:

  Hedge mode, OPEN fill (closedSize == 0):  Buy → 1 (long),  Sell → 2 (short)
  Hedge mode, CLOSE fill (closedSize > 0):  Buy → 2 (closes short), Sell → 1 (closes long)

This was the bug behind "Unmatched exit qty": closing fills carry the side
opposite to the position they close, so the naive Buy→1/Sell→2 mapping pointed
exit fills at the wrong position bucket.
"""

from models import Fill
from constants import PositionMode


def infer_position_idx(side: str, raw_idx: int, closed_size: float,
                       mode: PositionMode) -> int:
    if mode != PositionMode.HEDGE or raw_idx != 0:
        return raw_idx
    is_close = closed_size > 0
    if side == "Buy":
        return 2 if is_close else 1
    return 1 if is_close else 2


def parse_fill_ws(raw: dict, mode: PositionMode) -> Fill:
    closed_size = float(raw.get("closedSize", "0") or 0)
    idx = infer_position_idx(raw["side"], int(raw.get("positionIdx", "0")),
                             closed_size, mode)
    return Fill(
        exec_id=raw["execId"],
        order_id=raw["orderId"],
        symbol=raw["symbol"],
        side=raw["side"],
        exec_price=float(raw["execPrice"]),
        exec_qty=float(raw["execQty"]),
        exec_fee=float(raw.get("execFee", "0") or 0),
        exec_time_ms=int(raw["execTime"]),
        exec_type=raw.get("execType", "Trade"),
        position_idx=idx,
        closed_size=closed_size,
        order_type=raw.get("orderType", ""),
        stop_order_type=raw.get("stopOrderType", ""),
        is_maker=str(raw.get("isMaker", "false")).lower() == "true",
    )


def parse_fill_rest(raw: dict, mode: PositionMode) -> Fill:
    closed_size = float(raw.get("closedSize", "0") or 0)
    idx = infer_position_idx(raw["side"], int(raw.get("positionIdx", "0")),
                             closed_size, mode)
    return Fill(
        exec_id=raw["execId"],
        order_id=raw["orderId"],
        symbol=raw["symbol"],
        side=raw["side"],
        exec_price=float(raw["execPrice"]),
        exec_qty=float(raw["execQty"]),
        exec_fee=float(raw.get("execFee", "0") or 0),
        exec_time_ms=int(raw["execTime"]),
        exec_type=raw.get("execType", "Trade"),
        position_idx=idx,
        closed_size=closed_size,
        order_type=raw.get("orderType", ""),
        stop_order_type=raw.get("stopOrderType", ""),
        is_maker=bool(raw.get("isMaker", False)),
    )
