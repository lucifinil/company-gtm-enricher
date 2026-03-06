import pandas as pd

from company_gtm_enricher.enrichment_service import CompanyEnrichmentService
from company_gtm_enricher.models import AUDIT_COLUMN_MAP, ENRICHMENT_COLUMN_MAP, CompanyEnrichment
from company_gtm_enricher.providers.mock import MockEnrichmentProvider


def test_enrich_dataframe_appends_expected_columns() -> None:
    dataframe = pd.DataFrame({"company": ["OpenAI", "PingCAP"]})

    enriched = CompanyEnrichmentService(provider=MockEnrichmentProvider()).enrich_dataframe(
        dataframe=dataframe,
        company_column="company",
    )

    for column_name in ENRICHMENT_COLUMN_MAP.values():
        assert column_name in enriched.columns

    for column_name in AUDIT_COLUMN_MAP.values():
        assert column_name in enriched.columns

    assert enriched.loc[0, "HQ City"] == "San Francisco"
    assert enriched.loc[1, "Current Total Funding"] == "$290M+"


def test_enrich_dataframe_skips_blank_rows() -> None:
    dataframe = pd.DataFrame({"company": ["OpenAI", ""]})

    enriched = CompanyEnrichmentService(provider=MockEnrichmentProvider()).enrich_dataframe(
        dataframe=dataframe,
        company_column="company",
    )

    assert enriched.loc[1, "Enrichment Status"] == "skipped_empty"
    assert enriched.loc[1, "HQ City"] == ""


def test_enrich_dataframe_uses_batch_provider_for_unique_companies() -> None:
    provider = RecordingBatchProvider()
    dataframe = pd.DataFrame({"company": ["OpenAI", "PingCAP", "OpenAI"]})

    enriched = CompanyEnrichmentService(provider=provider).enrich_dataframe(
        dataframe=dataframe,
        company_column="company",
        batch_size=2,
    )

    assert provider.batch_calls == [["OpenAI", "PingCAP"]]
    assert provider.single_calls == []
    assert enriched.loc[2, "HQ City"] == "OpenAI City"


def test_enrich_dataframe_falls_back_to_single_calls_when_batch_fails() -> None:
    provider = FailingBatchProvider()
    dataframe = pd.DataFrame({"company": ["OpenAI", "PingCAP"]})

    enriched = CompanyEnrichmentService(provider=provider).enrich_dataframe(
        dataframe=dataframe,
        company_column="company",
        batch_size=2,
    )

    assert provider.batch_calls == [["OpenAI", "PingCAP"]]
    assert provider.single_calls == ["OpenAI", "PingCAP"]
    assert enriched.loc[0, "HQ City"] == "OpenAI Single City"


class RecordingBatchProvider:
    def __init__(self) -> None:
        self.batch_calls = []
        self.single_calls = []

    def enrich_company(self, company_name: str) -> CompanyEnrichment:
        self.single_calls.append(company_name)
        return CompanyEnrichment(
            company_name=company_name,
            hq_city=f"{company_name} Single City",
        )

    def enrich_companies(self, company_names):
        self.batch_calls.append(list(company_names))
        return {
            company_name: CompanyEnrichment(
                company_name=company_name,
                hq_city=f"{company_name} City",
            )
            for company_name in company_names
        }


class FailingBatchProvider(RecordingBatchProvider):
    def enrich_companies(self, company_names):
        self.batch_calls.append(list(company_names))
        raise RuntimeError("batch failure")
