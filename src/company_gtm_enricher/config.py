from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class AppConfig:
    openai_api_key: str
    openai_model: str
    openai_timeout_seconds: float
    max_companies_per_run: int

    @classmethod
    def from_env(cls) -> "AppConfig":
        load_dotenv()
        return cls(
            openai_api_key=os.getenv("OPENAI_API_KEY", "").strip(),
            openai_model=os.getenv("OPENAI_MODEL", "gpt-4o-mini-search-preview").strip(),
            openai_timeout_seconds=float(os.getenv("OPENAI_TIMEOUT_SECONDS", "45")),
            max_companies_per_run=int(os.getenv("MAX_COMPANIES_PER_RUN", "25")),
        )
