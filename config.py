"""Configuration from environment variables.

The bot now writes through the shared Apps Script doPost web app (the single
authoritative writer that also serves cTrader/Capital), so it no longer needs
Google credentials or a sheet id — only the web-app URL and the shared secret.
"""

import json
import os
import logging
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger(__name__)


@dataclass
class AccountConfig:
    name: str
    api_key: str
    api_secret: str
    category: str = "linear"
    force_env: Optional[str] = None      # "mainnet" | "demo" | "testnet"
    force_mode: Optional[str] = None     # "OneWay" | "Hedge"


@dataclass
class DoPostConfig:
    url: str
    secret: str


@dataclass
class AppConfig:
    accounts: list[AccountConfig]
    dopost: DoPostConfig
    log_level: str = "INFO"


def _field(acc: dict, *keys: str) -> str:
    for key in keys:
        if key in acc and acc[key] not in (None, ""):
            return acc[key]
    raise KeyError(f"Missing required field. Tried {keys}. Got keys: {list(acc.keys())}")


def load_config() -> AppConfig:
    raw = os.getenv("ACCOUNTS_JSON", "[]")
    try:
        accounts_data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid ACCOUNTS_JSON: {e}")
    if not accounts_data:
        raise ValueError("ACCOUNTS_JSON is empty — no accounts configured")

    accounts: list[AccountConfig] = []
    for i, acc in enumerate(accounts_data):
        try:
            accounts.append(AccountConfig(
                name=_field(acc, "name", "Name", "account_name"),
                api_key=_field(acc, "api_key", "apiKey", "key"),
                api_secret=_field(acc, "api_secret", "apiSecret", "secret"),
                category=acc.get("category", "linear"),
                force_env=acc.get("env") or acc.get("force_env"),
                force_mode=acc.get("position_mode") or acc.get("mode"),
            ))
        except KeyError as e:
            raise ValueError(f"Account #{i + 1} config error: {e}")

    url = os.getenv("DOPOST_URL", "")
    if not url:
        raise ValueError("DOPOST_URL environment variable is required "
                         "(the Apps Script web-app deployment URL)")
    secret = os.getenv("DOPOST_SECRET", "dsbfb@dfshds3434")

    return AppConfig(
        accounts=accounts,
        dopost=DoPostConfig(url=url, secret=secret),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
