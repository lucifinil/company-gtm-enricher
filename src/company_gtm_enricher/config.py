from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv


@dataclass(frozen=True)
class AppConfig:
    openai_api_key: str
    openai_model: str
    openai_timeout_seconds: float
    openai_batch_size: int
    max_companies_per_run: Optional[int]

    @classmethod
    def from_env(cls) -> "AppConfig":
        load_dotenv()
        return cls(
            openai_api_key=os.getenv("OPENAI_API_KEY", "").strip(),
            openai_model=os.getenv("OPENAI_MODEL", "gpt-4o-mini-search-preview").strip(),
            openai_timeout_seconds=float(os.getenv("OPENAI_TIMEOUT_SECONDS", "45")),
            openai_batch_size=_parse_batch_size(os.getenv("OPENAI_BATCH_SIZE", "10").strip()),
            max_companies_per_run=_parse_max_companies_per_run(
                os.getenv("MAX_COMPANIES_PER_RUN", "").strip()
            ),
        )


def _parse_batch_size(raw_value: str) -> int:
    parsed = int(raw_value or "10")
    return parsed if parsed > 0 else 1


def _parse_max_companies_per_run(raw_value: str) -> Optional[int]:
    if not raw_value:
        return None
    parsed = int(raw_value)
    if parsed <= 0:
        return None
    return parsed
