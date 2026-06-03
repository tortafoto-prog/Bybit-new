"""Small shared helpers."""

from datetime import datetime, timezone
from typing import Optional

_DATE_FMT = "%Y.%m.%d. %H:%M:%S"


def ts_str(ms: Optional[int]) -> str:
    """Format an epoch-ms timestamp as 'YYYY.MM.DD. HH:MM:SS' (UTC)."""
    if not ms or ms <= 0:
        return ""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(_DATE_FMT)
