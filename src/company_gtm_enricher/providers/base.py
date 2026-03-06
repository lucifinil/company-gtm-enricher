from __future__ import annotations

from typing import Protocol

from company_gtm_enricher.models import CompanyEnrichment


class EnrichmentProvider(Protocol):
    def enrich_company(self, company_name: str) -> CompanyEnrichment:
        """Return enrichment data for the provided company name."""
