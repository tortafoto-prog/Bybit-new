"""Configuration loading from environment variables.

Accounts only need name/api_key/api_secret. The environment (mainnet/demo/
testnet) and position mode are auto-detected at startup, so the user never has
to specify them. Optional overrides are still honored if present.
"""

import json
import os
import logging
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)


@dataclass
class AccountConfig:
    name: str
    api_key: str
    api_secret: str
    category: str = "linear"
    # Optional explicit overrides (None = auto-detect)
    force_env: Optional[str] = None      # "mainnet" | "demo" | "testnet"
    force_mode: Optional[str] = None     # "OneWay" | "Hedge"


@dataclass
class SheetsConfig:
    credentials_json: dict
    sheet_id: str
    sheet_name: str = "Trades"


@dataclass
class AppConfig:
    accounts: list[AccountConfig]
    sheets: SheetsConfig
    log_level: str = "INFO"


def _field(acc: dict, *keys: str, required: bool = True) -> Optional[str]:
    for key in keys:
        if key in acc and acc[key] not in (None, ""):
            return acc[key]
    if required:
        raise KeyError(
            f"Missing required field. Tried {keys}. Got keys: {list(acc.keys())}"
        )
    return None


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

    creds_raw = os.getenv("GOOGLE_CREDS_JSON") or os.getenv("GOOGLE_CREDENTIALS", "{}")
    try:
        creds_json = json.loads(creds_raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid Google credentials JSON: {e}")

    sheet_id = os.getenv("SHEET_ID", "")
    if not sheet_id:
        raise ValueError("SHEET_ID environment variable is required")

    sheets = SheetsConfig(
        credentials_json=creds_json,
        sheet_id=sheet_id,
        sheet_name=os.getenv("SHEET_NAME", "Trades"),
    )

    return AppConfig(
        accounts=accounts,
        sheets=sheets,
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
