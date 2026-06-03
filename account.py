"""Wires together one account's components and runs its lifecycle."""

import asyncio
import logging
import time

from config import AccountConfig
from constants import PositionMode, TradeStatus
from models import AccountState
from bybit_rest import BybitREST
from bybit_ws import BybitWebSocket
from handlers import Dispatcher, ExecutionHandler, OrderHandler, PositionHandler
from trade_engine import TradeEngine
from sl_tracker import SLTracker
from recovery import RecoveryEngine
from journal import Journal

log = logging.getLogger(__name__)


class AccountManager:
    def __init__(self, config: AccountConfig, rest: BybitREST,
                 env: str, mode: PositionMode, journal: Journal):
        self.config = config
        self.name = config.name
        self.env = env
        self.mode = mode
        self.rest = rest
        self.journal = journal

        self.state = AccountState(account_name=config.name, position_mode=mode)
        self.sl = SLTracker(self.state, journal)
        self.engine = TradeEngine(self.state, self.sl, journal)
        self.recovery = RecoveryEngine(self.state, rest, self.engine, self.sl, mode)

        self.dispatcher = Dispatcher(config.name)
        self.dispatcher.register("execution", ExecutionHandler(config.name, self.engine, mode).handle)
        self.dispatcher.register("order", OrderHandler(config.name, self.sl, mode).handle)
        self.dispatcher.register("position", PositionHandler(config.name, self.state, self.sl).handle)

        self.ws = BybitWebSocket(
            api_key=config.api_key, api_secret=config.api_secret, account_name=config.name,
            testnet=(env == "testnet"), demo=(env == "demo"),
            on_message=self.dispatcher.dispatch,
            on_connect=self._on_connect, on_disconnect=self._on_disconnect,
        )

    async def start(self):
        log.info(f"[{self.name}] Starting ({self.env}, {self.mode.value})")
        # No sheet read: open-trade state is rebuilt from REST history by recovery,
        # and doPost dedups by Ticket ID so re-posting recovered trades is a no-op.
        await self.recovery.validate_current_positions()
        await self.recovery.recover(force_full=True)
        await self.ws.connect()   # blocks: connect/reconnect loop

    async def stop(self):
        log.info(f"[{self.name}] Stopping")
        await self.ws.disconnect()
        await self.sl.cleanup()

    async def _on_connect(self):
        log.info(f"[{self.name}] WebSocket connected")
        self.state.ws_connected = True
        if self.state.last_ws_disconnect_ms:
            await self.recovery.recover(gap_start_ms=self.state.last_ws_disconnect_ms)
            self.state.last_ws_disconnect_ms = None

    async def _on_disconnect(self):
        log.warning(f"[{self.name}] WebSocket disconnected")
        self.state.ws_connected = False
        self.state.last_ws_disconnect_ms = int(time.time() * 1000)
