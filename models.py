"""Core data models for the trade journal (v2)."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

from constants import TradeStatus, ExitType, VOLUME_TOLERANCE


@dataclass(frozen=True)
class PositionKey:
    """Identifies a Bybit position bucket: symbol + positionIdx.

    positionIdx: 0 = one-way, 1 = hedge long, 2 = hedge short.
    """
    symbol: str
    position_idx: int

    def __str__(self) -> str:
        return f"{self.symbol}:{self.position_idx}"


@dataclass
class Fill:
    """A single execution/fill (from WS execution topic or REST execution list)."""
    exec_id: str
    order_id: str
    symbol: str
    side: str               # "Buy" | "Sell"
    exec_price: float
    exec_qty: float
    exec_fee: float
    exec_time_ms: int
    exec_type: str          # "Trade" | "BustTrade" | "Funding" | ...
    position_idx: int
    closed_size: float      # >0 → this fill (partly) closes a position
    order_type: str         # "Market" | "Limit" | ...
    stop_order_type: str    # "" | "StopLoss" | "TakeProfit" | ...
    is_maker: bool = False

    @property
    def is_open_fill(self) -> bool:
        return self.open_qty > VOLUME_TOLERANCE

    @property
    def is_close_fill(self) -> bool:
        return self.closed_size > VOLUME_TOLERANCE

    @property
    def open_qty(self) -> float:
        return max(0.0, self.exec_qty - self.closed_size)


@dataclass
class SLEvent:
    symbol: str
    side: str               # POSITION side ("Buy" long / "Sell" short)
    position_idx: int
    sl_price: float         # 0.0 = removed
    sl_order_id: Optional[str]
    timestamp_ms: int
    source: str             # "order_ws" | "position_ws" | "rest_recovery"


@dataclass
class TPEvent:
    symbol: str
    side: str
    position_idx: int
    tp_price: float         # 0.0 = removed
    tp_order_id: Optional[str]
    timestamp_ms: int
    source: str


@dataclass
class PositionSnapshot:
    key: PositionKey
    side: str
    size: float
    entry_price: float
    sl_price: float
    tp_price: float
    leverage: float
    liq_price: float
    updated_at_ms: int


@dataclass
class InternalTrade:
    """One logical trade — the unit we journal. One entry order = one trade."""
    trade_id: str
    account_name: str
    symbol: str
    side: str               # "Buy" (long) | "Sell" (short)
    position_idx: int

    # Entry
    entry_price: float
    entry_qty: float
    entry_time_ms: int
    entry_exec_id: str
    entry_order_id: str
    entry_fee: float = 0.0
    entry_order_type: str = ""   # "Market" | "Limit" | "Stop" | ...

    # Stop-loss
    sl_price: Optional[float] = None
    sl_assigned_time_ms: Optional[int] = None
    sl_source: Optional[str] = None

    # Take-profit
    tp_price: Optional[float] = None
    tp_assigned_time_ms: Optional[int] = None
    tp_source: Optional[str] = None

    # Exit
    exit_price: Optional[float] = None
    exit_qty: float = 0.0
    exit_time_ms: Optional[int] = None
    exit_exec_ids: list[str] = field(default_factory=list)
    exit_fee: float = 0.0
    exit_type: Optional[ExitType] = None

    # State
    status: TradeStatus = TradeStatus.PENDING
    rr_ratio: Optional[float] = None
    pnl: Optional[float] = None

    # Bookkeeping
    grace_deadline_ms: Optional[int] = None
    sheets_open_row: Optional[int] = None
    sheets_close_row: Optional[int] = None
    created_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    updated_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))

    @property
    def pos_key(self) -> PositionKey:
        return PositionKey(self.symbol, self.position_idx)

    @property
    def remaining_qty(self) -> float:
        return max(0.0, self.entry_qty - self.exit_qty)

    @property
    def is_fully_closed(self) -> bool:
        return abs(self.exit_qty - self.entry_qty) < VOLUME_TOLERANCE

    @property
    def risk(self) -> Optional[float]:
        if self.sl_price is None:
            return None
        return abs(self.entry_price - self.sl_price)

    @property
    def reward(self) -> Optional[float]:
        if self.exit_price is None:
            return None
        return abs(self.exit_price - self.entry_price)

    def calculate_rr(self) -> Optional[float]:
        r = self.risk
        if r and r > 0 and self.reward is not None:
            return round(self.reward / r, 4)
        return None

    def calculate_pnl(self) -> Optional[float]:
        if self.exit_price is None:
            return None
        if self.side == "Buy":
            raw = (self.exit_price - self.entry_price) * self.exit_qty
        else:
            raw = (self.entry_price - self.exit_price) * self.exit_qty
        return round(raw - self.entry_fee - self.exit_fee, 6)


@dataclass
class AccountState:
    """All mutable runtime state for one account."""
    account_name: str
    position_mode: "object" = None  # PositionMode, set at startup

    open_trades: dict[str, InternalTrade] = field(default_factory=dict)

    # PositionKey -> ordered trade_ids (oldest first, newest last)
    pending_sl_queue: dict[PositionKey, list[str]] = field(default_factory=dict)
    pending_tp_queue: dict[PositionKey, list[str]] = field(default_factory=dict)

    last_known_sl: dict[PositionKey, float] = field(default_factory=dict)
    last_known_tp: dict[PositionKey, float] = field(default_factory=dict)

    positions: dict[PositionKey, PositionSnapshot] = field(default_factory=dict)

    # Short-lived buffers for events that arrive before their fill
    recent_unmatched_sl: dict[PositionKey, SLEvent] = field(default_factory=dict)
    recent_unmatched_tp: dict[PositionKey, TPEvent] = field(default_factory=dict)

    seen_exec_ids: set[str] = field(default_factory=set)

    last_exec_time_ms: int = 0
    ws_connected: bool = False
    last_ws_disconnect_ms: Optional[int] = None
