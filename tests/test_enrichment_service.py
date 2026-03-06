import pandas as pd

from company_gtm_enricher.enrichment_service import CompanyEnrichmentService
from company_gtm_enricher.models import AUDIT_COLUMN_MAP, ENRICHMENT_COLUMN_MAP
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
