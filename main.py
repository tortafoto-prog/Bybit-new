"""Entry point: connect Sheets, auto-detect each account's environment, run all."""

import asyncio
import logging
import signal
import sys

from dotenv import load_dotenv

load_dotenv()

from config import load_config
from constants import PositionMode
from bybit_rest import BybitREST
from dopost_client import DoPostClient
from journal import Journal
from account import AccountManager

log = logging.getLogger("bybit_journal")


def setup_logging(level: str = "INFO"):
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    root.addHandler(handler)


async def main():
    config = load_config()
    setup_logging(config.log_level)
    log.info(f"Starting Bybit Trade Journal with {len(config.accounts)} accounts")

    dopost = DoPostClient(url=config.dopost.url, secret=config.dopost.secret)
    journal = Journal(dopost)
    await journal.start()
    log.info(f"Journaling via doPost web app: {config.dopost.url}")

    # dedup account names
    seen, accounts = set(), []
    for acc in config.accounts:
        if acc.name in seen:
            log.warning(f"Duplicate account '{acc.name}' — skipping")
            continue
        seen.add(acc.name)
        accounts.append(acc)

    # auto-detect environment + mode per account
    managers: list[AccountManager] = []
    for acc in accounts:
        try:
            rest, env, mode_str = await BybitREST.auto_detect(
                api_key=acc.api_key, api_secret=acc.api_secret,
                account_name=acc.name, category=acc.category, force_env=acc.force_env)
        except ValueError as e:
            log.error(str(e))
            continue
        mode = PositionMode(acc.force_mode) if acc.force_mode else PositionMode(mode_str)
        log.info(f"[{acc.name}] Environment: {env}, Position mode: {mode.value}")
        managers.append(AccountManager(acc, rest, env, mode, journal))

    log.info(f"Starting with {len(managers)} valid account(s)")

    shutdown = asyncio.Event()

    def _sig():
        log.info("Shutdown signal received")
        shutdown.set()

    loop = asyncio.get_event_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _sig)
        except NotImplementedError:
            signal.signal(s, lambda *_: _sig())

    tasks = []
    for i, m in enumerate(managers):
        if i > 0:
            await asyncio.sleep(2)   # stagger to avoid IP rate limits
        tasks.append(asyncio.create_task(m.start()))

    await shutdown.wait()

    log.info("Shutting down...")
    for m in managers:
        await m.stop()
    await journal.stop()
    for t in tasks:
        t.cancel()
    log.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
