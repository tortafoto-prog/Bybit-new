"""HTTP client for the shared Apps Script doPost web app.

Posts trade rows exactly like the cTrader/Capital integrations do. The web app
handles validation, Ticket-ID dedup, the Trades sheet, Tracker sync, Discord and
the Log tab — so the bot stays "dumb" and consistent with the other sources.

`requests` ships as a transitive dependency of pybit, so no extra package.
"""

import asyncio
import logging
from typing import Optional

import requests

log = logging.getLogger(__name__)

# Apps Script web-app responds 302→200; allow redirects and retry on network errors.
_TIMEOUT = 20
_MAX_RETRIES = 3
_RETRY_BASE_S = 1.5
_MIN_INTERVAL_S = 0.3   # gap between posts (shared across accounts)

_lock = asyncio.Lock()


class DoPostClient:
    def __init__(self, url: str, secret: str):
        self.url = url
        self.secret = secret

    async def post(self, payload: dict) -> dict:
        """POST one trade payload. Returns the parsed JSON response (or an error
        dict). Never raises — journaling must not crash the engine."""
        body = dict(payload)
        body["secret_key"] = self.secret
        body.setdefault("source", "bybit")

        last_err: Optional[Exception] = None
        for attempt in range(_MAX_RETRIES):
            async with _lock:
                try:
                    resp = await asyncio.get_event_loop().run_in_executor(
                        None, self._post_sync, body)
                    await asyncio.sleep(_MIN_INTERVAL_S)
                    return resp
                except Exception as e:
                    last_err = e
            backoff = _RETRY_BASE_S * (2 ** attempt)
            log.warning(f"doPost network error (attempt {attempt + 1}/{_MAX_RETRIES}): "
                        f"{last_err} — retry in {backoff}s")
            await asyncio.sleep(backoff)
        log.error(f"doPost failed permanently: {last_err}")
        return {"status": "error", "message": str(last_err)}

    def _post_sync(self, body: dict) -> dict:
        r = requests.post(self.url, json=body, timeout=_TIMEOUT, allow_redirects=True)
        r.raise_for_status()
        try:
            return r.json()
        except ValueError:
            return {"status": "error", "message": f"non-JSON response: {r.text[:200]}"}
