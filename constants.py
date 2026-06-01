"""Enums and tunable constants for the Bybit trade journal (v2)."""

from enum import Enum


class TradeStatus(str, Enum):
    PENDING = "PENDING"   # Entry filled, waiting for SL within grace period
    ACTIVE = "ACTIVE"     # Open with a valid SL
    INVALID = "INVALID"   # Open but no valid SL (still volume-tracked, flagged in sheet)
    CLOSED = "CLOSED"     # Fully closed and journaled


class ExitType(str, Enum):
    SL_HIT = "SL_HIT"
    TP_HIT = "TP_HIT"
    MANUAL = "MANUAL"
    LIQUIDATION = "LIQUIDATION"
    UNKNOWN = "UNKNOWN"


class PositionMode(str, Enum):
    ONE_WAY = "OneWay"
    HEDGE = "Hedge"


# ── Timing (milliseconds unless noted) ──────────────────────────────────────
SL_GRACE_PERIOD_MS = 60_000          # No SL within this window after entry → INVALID
SL_UNMATCHED_BUFFER_TTL_MS = 10_000  # Buffer an SL/TP that arrives before its fill

WS_PING_INTERVAL_S = 20
WS_PONG_TIMEOUT_S = 10
WS_RECONNECT_BASE_S = 1
WS_RECONNECT_MAX_S = 30

# ── Sheets ──────────────────────────────────────────────────────────────────
SHEETS_FLUSH_INTERVAL_S = 2
SHEETS_BATCH_SIZE = 20

# ── Recovery ────────────────────────────────────────────────────────────────
RECOVERY_MAX_WINDOW_DAYS = 1   # Bybit only keeps cancelled SL orders ~24h
RECOVERY_PAGE_WINDOW_DAYS = 1

# ── Numeric tolerance ───────────────────────────────────────────────────────
QTY_TOLERANCE = 1e-6
VOLUME_TOLERANCE = 1e-10
