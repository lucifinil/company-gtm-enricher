from __future__ import annotations

from typing import Dict, Protocol, Sequence

from company_gtm_enricher.models import CompanyEnrichment


class EnrichmentProvider(Protocol):
    def enrich_company(self, company_name: str) -> CompanyEnrichment:
        """Return enrichment data for the provided company name."""

    def enrich_companies(self, company_names: Sequence[str]) -> Dict[str, CompanyEnrichment]:
        """Return enrichment data keyed by the original company names."""
