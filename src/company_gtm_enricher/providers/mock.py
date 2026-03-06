from __future__ import annotations

from typing import Dict, Sequence

from company_gtm_enricher.models import CompanyEnrichment


MOCK_COMPANY_DATA = {
    "openai": CompanyEnrichment(
        company_name="OpenAI",
        hq_city="San Francisco",
        hq_state="California",
        country="United States",
        approximate_annual_revenue="$3B-$4B",
        current_total_funding="$20B+",
        confidence="medium",
        source_urls=["https://openai.com", "https://www.crunchbase.com/organization/openai"],
        notes="Mock data for workflow testing.",
    ),
    "pingcap": CompanyEnrichment(
        company_name="PingCAP",
        hq_city="San Mateo",
        hq_state="California",
        country="United States",
        approximate_annual_revenue="$50M-$100M",
        current_total_funding="$290M+",
        confidence="medium",
        source_urls=["https://www.pingcap.com", "https://www.crunchbase.com/organization/pingcap"],
        notes="Mock data for workflow testing.",
    ),
    "snowflake": CompanyEnrichment(
        company_name="Snowflake",
        hq_city="Bozeman",
        hq_state="Montana",
        country="United States",
        approximate_annual_revenue="$3B+",
        current_total_funding="Public company",
        confidence="medium",
        source_urls=["https://www.snowflake.com"],
        notes="Mock data for workflow testing.",
    ),
}


class MockEnrichmentProvider:
    def enrich_company(self, company_name: str) -> CompanyEnrichment:
        cached = MOCK_COMPANY_DATA.get(company_name.casefold())
        if cached is not None:
            return cached

        return CompanyEnrichment(
            company_name=company_name,
            hq_city="Unknown",
            hq_state="Unknown",
            country="Unknown",
            approximate_annual_revenue="Unknown",
            current_total_funding="Unknown",
            confidence="low",
            source_urls=[],
            notes="Mock provider does not have a curated entry for this company.",
        )

    def enrich_companies(self, company_names: Sequence[str]) -> Dict[str, CompanyEnrichment]:
        return {company_name: self.enrich_company(company_name) for company_name in company_names}
