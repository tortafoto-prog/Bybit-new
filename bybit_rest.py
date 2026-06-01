"""Bybit V5 REST client.

Design decisions that fix observed production problems:
- Uses the `bytick` mirror domain on mainnet — Bybit blocks many cloud/shared
  IPs (incl. Railway) on api.bybit.com with a 403 "ip rate limit / usa" error,
  but api.bytick.com serves the same API from a different edge.
- A single *global* lock + min-interval throttle shared across ALL accounts,
  because Bybit rate-limits per IP, not per key.
- Retry with exponential backoff on transient 403/rate-limit errors.
- auto_detect(): finds whether a key lives on mainnet / demo / testnet by
  looking for actual activity (open positions, then recent executions), so the
  operator never has to declare the environment.
"""

import asyncio
import logging
import time
from typing import Optional

from pybit.unified_trading import HTTP
from pybit.exceptions import FailedRequestError, InvalidRequestError

log = logging.getLogger(__name__)

# Shared across every BybitREST instance (per-IP rate limiting on Bybit's side).
_global_lock = asyncio.Lock()
_MIN_INTERVAL_S = 0.35
_MAX_RETRIES = 3
_RETRY_BASE_S = 2.0

_ENVIRONMENTS = [
    # (name, testnet, demo)
    ("mainnet", False, False),
    ("demo", False, True),
    ("testnet", True, False),
]


def _is_rate_limit(err: Exception) -> bool:
    s = str(err).lower()
    return "403" in s or "rate limit" in s


class BybitREST:
    """Async wrapper around the synchronous pybit HTTP client."""

    def __init__(self, api_key: str, api_secret: str, account_name: str,
                 testnet: bool = False, demo: bool = False, category: str = "linear"):
        self.account_name = account_name
        self.category = category
        self.testnet = testnet
        self.demo = demo
        kwargs = dict(api_key=api_key, api_secret=api_secret, testnet=testnet)
        if demo:
            kwargs["demo"] = True
        elif not testnet:
            kwargs["domain"] = "bytick"
        self._client = HTTP(**kwargs)

    # ── low-level call with global throttle + retry ─────────────────────────
    async def _call(self, method, **kwargs):
        last_err: Optional[Exception] = None
        for attempt in range(_MAX_RETRIES):
            async with _global_lock:
                loop = asyncio.get_event_loop()
                try:
                    result = await loop.run_in_executor(None, lambda: method(**kwargs))
                    await asyncio.sleep(_MIN_INTERVAL_S)
                    return result
                except (FailedRequestError, InvalidRequestError) as e:
                    last_err = e
                    if not _is_rate_limit(e) or attempt == _MAX_RETRIES - 1:
                        raise
                    await asyncio.sleep(_MIN_INTERVAL_S)
            backoff = _RETRY_BASE_S * (2 ** attempt)
            log.warning(f"[{self.account_name}] Rate limited, retry in {backoff}s "
                        f"({attempt + 1}/{_MAX_RETRIES})")
            await asyncio.sleep(backoff)
        raise last_err  # type: ignore[misc]

    # ── positions ───────────────────────────────────────────────────────────
    async def get_positions(self, settle_coin: str = "USDT") -> list[dict]:
        result = await self._call(
            self._client.get_positions,
            category=self.category, settleCoin=settle_coin,
        )
        return result.get("result", {}).get("list", [])

    # ── executions ──────────────────────────────────────────────────────────
    async def get_executions(self, start_time: Optional[int] = None,
                             end_time: Optional[int] = None,
                             limit: int = 100, cursor: Optional[str] = None) -> dict:
        kwargs = {"category": self.category, "limit": limit}
        if start_time:
            kwargs["startTime"] = start_time
        if end_time:
            kwargs["endTime"] = end_time
        if cursor:
            kwargs["cursor"] = cursor
        result = await self._call(self._client.get_executions, **kwargs)
        return result.get("result", {})

    async def get_all_executions(self, start_time: Optional[int] = None,
                                 end_time: Optional[int] = None) -> list[dict]:
        items, cursor = [], None
        while True:
            res = await self.get_executions(start_time, end_time, cursor=cursor)
            items.extend(res.get("list", []))
            cursor = res.get("nextPageCursor", "")
            if not cursor:
                break
        return items

    # ── order history ─────────────────────────────────────────────────────--
    async def get_order_history(self, order_filter: Optional[str] = None,
                                start_time: Optional[int] = None,
                                end_time: Optional[int] = None,
                                limit: int = 50, cursor: Optional[str] = None) -> dict:
        kwargs = {"category": self.category, "limit": limit}
        if order_filter:
            kwargs["orderFilter"] = order_filter
        if start_time:
            kwargs["startTime"] = start_time
        if end_time:
            kwargs["endTime"] = end_time
        if cursor:
            kwargs["cursor"] = cursor
        result = await self._call(self._client.get_order_history, **kwargs)
        return result.get("result", {})

    async def get_all_order_history(self, order_filter: Optional[str] = None,
                                    start_time: Optional[int] = None,
                                    end_time: Optional[int] = None) -> list[dict]:
        items, cursor = [], None
        while True:
            res = await self.get_order_history(order_filter, start_time, end_time, cursor=cursor)
            items.extend(res.get("list", []))
            cursor = res.get("nextPageCursor", "")
            if not cursor:
                break
        return items

    # ── environment / mode detection ────────────────────────────────────────
    async def detect_mode_and_activity(self) -> tuple[str, bool]:
        """Return (position_mode, has_open_positions)."""
        result = await self._call(
            self._client.get_positions,
            category=self.category, settleCoin="USDT",
        )
        positions = result.get("result", {}).get("list", [])
        mode, has_open = "OneWay", False
        for pos in positions:
            if float(pos.get("size", "0") or 0) > 0:
                has_open = True
            if int(pos.get("positionIdx", "0")) in (1, 2):
                mode = "Hedge"
        return mode, has_open

    async def has_recent_executions(self, days: int = 7) -> bool:
        now = int(time.time() * 1000)
        res = await self.get_executions(now - days * 86_400_000, now, limit=1)
        return len(res.get("list", [])) > 0

    @staticmethod
    async def auto_detect(api_key: str, api_secret: str, account_name: str,
                          category: str = "linear",
                          force_env: Optional[str] = None
                          ) -> tuple["BybitREST", str, str]:
        """Locate the environment a key belongs to and its position mode.

        Returns (client, env_name, position_mode). Prefers the environment with
        live open positions, then one with recent executions, then the first
        valid one. Raises ValueError if the key is invalid everywhere.
        """
        envs = _ENVIRONMENTS
        if force_env:
            envs = [e for e in _ENVIRONMENTS if e[0] == force_env] or _ENVIRONMENTS

        valid: list[tuple[str, "BybitREST", str]] = []
        for env_name, testnet, demo in envs:
            client = BybitREST(api_key, api_secret, account_name,
                               testnet=testnet, demo=demo, category=category)
            try:
                mode, has_open = await client.detect_mode_and_activity()
            except Exception:
                continue
            log.info(f"[{account_name}] Key valid on {env_name} (open positions: {has_open})")
            if has_open or force_env:
                return client, env_name, mode
            valid.append((env_name, client, mode))

        for env_name, client, mode in valid:
            try:
                if await client.has_recent_executions():
                    log.info(f"[{account_name}] Recent activity on {env_name}")
                    return client, env_name, mode
            except Exception:
                continue

        if valid:
            env_name, client, mode = valid[0]
            return client, env_name, mode

        raise ValueError(
            f"[{account_name}] API key not valid on any environment "
            f"(mainnet/demo/testnet)"
        )
