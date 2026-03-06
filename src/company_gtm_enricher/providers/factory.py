from __future__ import annotations

from typing import Optional

from company_gtm_enricher.config import AppConfig
from company_gtm_enricher.providers.mock import MockEnrichmentProvider
from company_gtm_enricher.providers.openai_provider import OpenAIEnrichmentProvider


def create_provider(
    provider_name: str,
    config: AppConfig,
    model_override: Optional[str] = None,
):
    normalized_name = provider_name.strip().lower()
    if normalized_name == "mock":
        return MockEnrichmentProvider()
    if normalized_name == "openai":
        return OpenAIEnrichmentProvider(
            api_key=config.openai_api_key,
            model=model_override or config.openai_model,
            timeout_seconds=config.openai_timeout_seconds,
        )
    raise ValueError(f"Unsupported provider: {provider_name}")
