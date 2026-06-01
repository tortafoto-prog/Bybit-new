"""Bybit V5 private WebSocket client.

Uses the bytick stream mirror on mainnet (same reason as the REST client), and
the dedicated demo stream for demo accounts. Handles auth, subscribe, heartbeat
and reconnect-with-backoff. Each decoded message is handed to `on_message`.
"""

import asyncio
import hashlib
import hmac
import json
import logging
import time
from typing import Awaitable, Callable, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from constants import (
    WS_PING_INTERVAL_S, WS_PONG_TIMEOUT_S,
    WS_RECONNECT_BASE_S, WS_RECONNECT_MAX_S,
)

log = logging.getLogger(__name__)

WS_URL_MAINNET = "wss://stream.bytick.com/v5/private"
WS_URL_DEMO = "wss://stream-demo.bybit.com/v5/private"
WS_URL_TESTNET = "wss://stream-testnet.bybit.com/v5/private"

TOPICS = ["order", "execution", "position"]


class BybitWebSocket:
    def __init__(self, api_key: str, api_secret: str, account_name: str,
                 testnet: bool = False, demo: bool = False,
                 on_message: Optional[Callable[[dict], Awaitable[None]]] = None,
                 on_connect: Optional[Callable[[], Awaitable[None]]] = None,
                 on_disconnect: Optional[Callable[[], Awaitable[None]]] = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.account_name = account_name
        self.on_message = on_message
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect

        if demo:
            self.url = WS_URL_DEMO
        elif testnet:
            self.url = WS_URL_TESTNET
        else:
            self.url = WS_URL_MAINNET

        self._ws = None
        self._running = False
        self._reconnects = 0
        self._last_msg = 0.0

    def _sign(self) -> tuple[int, str]:
        expires = int((time.time() + 5) * 1000)
        sig = hmac.new(self.api_secret.encode(),
                       f"GET/realtime{expires}".encode(),
                       hashlib.sha256).hexdigest()
        return expires, sig

    async def _authenticate(self):
        expires, sig = self._sign()
        await self._ws.send(json.dumps({"op": "auth", "args": [self.api_key, expires, sig]}))
        resp = json.loads(await asyncio.wait_for(self._ws.recv(), timeout=10))
        if not resp.get("success"):
            raise ConnectionError(f"[{self.account_name}] Auth failed: {resp}")
        log.info(f"[{self.account_name}] WebSocket authenticated")

    async def _subscribe(self):
        await self._ws.send(json.dumps({"op": "subscribe", "args": TOPICS}))
        resp = json.loads(await asyncio.wait_for(self._ws.recv(), timeout=10))
        if not resp.get("success"):
            raise ConnectionError(f"[{self.account_name}] Subscribe failed: {resp}")
        log.info(f"[{self.account_name}] Subscribed to {TOPICS}")

    async def _heartbeat(self):
        """Bybit V5 expects an application-level {"op":"ping"} every 20s and
        replies {"op":"pong"}. We send that and treat the connection as dead
        only if no message (pong or data) arrives for a while — protocol-level
        ping/pong is unreliable on Bybit and caused spurious reconnects."""
        dead_after = WS_PING_INTERVAL_S + WS_PONG_TIMEOUT_S + 30
        while self._running and self._ws:
            try:
                await asyncio.sleep(WS_PING_INTERVAL_S)
                if not (self._ws and self._ws.open):
                    break
                await self._ws.send(json.dumps({"op": "ping"}))
                if self._last_msg and (time.time() - self._last_msg) > dead_after:
                    log.warning(f"[{self.account_name}] No messages for {dead_after}s → reconnect")
                    await self._ws.close()
                    break
            except (ConnectionClosed, Exception) as e:
                log.warning(f"[{self.account_name}] Heartbeat stopped: {e}")
                break

    async def _recv_loop(self):
        while self._running and self._ws:
            try:
                data = json.loads(await self._ws.recv())
                self._last_msg = time.time()
                if data.get("op") in ("pong", "ping", "subscribe", "auth"):
                    continue
                if self.on_message and "topic" in data:
                    try:
                        await self.on_message(data)
                    except Exception as e:
                        log.error(f"[{self.account_name}] Handler error: {e}", exc_info=True)
            except ConnectionClosed:
                log.warning(f"[{self.account_name}] WebSocket closed")
                break
            except Exception as e:
                log.error(f"[{self.account_name}] Recv error: {e}", exc_info=True)
                break

    async def connect(self):
        self._running = True
        while self._running:
            try:
                log.info(f"[{self.account_name}] Connecting to {self.url}")
                self._ws = await websockets.connect(self.url, ping_interval=None, close_timeout=5)
                await self._authenticate()
                await self._subscribe()
                self._reconnects = 0
                self._last_msg = time.time()
                if self.on_connect:
                    await self.on_connect()

                recv_task = asyncio.create_task(self._recv_loop())
                hb_task = asyncio.create_task(self._heartbeat())
                _, pending = await asyncio.wait(
                    [recv_task, hb_task], return_when=asyncio.FIRST_COMPLETED)
                for t in pending:
                    t.cancel()
                    try:
                        await t
                    except asyncio.CancelledError:
                        pass
            except Exception as e:
                log.error(f"[{self.account_name}] Connection error: {e}")

            if self.on_disconnect:
                try:
                    await self.on_disconnect()
                except Exception as e:
                    log.error(f"[{self.account_name}] Disconnect handler error: {e}")

            if not self._running:
                break
            delay = min(WS_RECONNECT_BASE_S * (2 ** self._reconnects), WS_RECONNECT_MAX_S)
            self._reconnects += 1
            log.info(f"[{self.account_name}] Reconnecting in {delay}s (#{self._reconnects})")
            await asyncio.sleep(delay)

    async def disconnect(self):
        self._running = False
        if self._ws:
            await self._ws.close()
