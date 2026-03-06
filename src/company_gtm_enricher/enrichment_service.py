from __future__ import annotations

from typing import Callable, Dict, Optional

import pandas as pd

from company_gtm_enricher.csv_tools import normalize_company_name, validate_company_values
from company_gtm_enricher.models import CompanyEnrichment
from company_gtm_enricher.providers.base import EnrichmentProvider

ProgressCallback = Callable[[int, int, str], None]


class CompanyEnrichmentService:
    def __init__(self, provider: EnrichmentProvider) -> None:
        self.provider = provider

    def enrich_dataframe(
        self,
        dataframe: pd.DataFrame,
        company_column: str,
        include_audit_columns: bool = True,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> pd.DataFrame:
        if company_column not in dataframe.columns:
            raise ValueError(f"Column `{company_column}` does not exist in the uploaded CSV.")

        validate_company_values(dataframe[company_column].tolist())
        total_rows = len(dataframe.index)
        cache: Dict[str, CompanyEnrichment] = {}
        enrichment_rows = []

        for index, raw_value in enumerate(dataframe[company_column].tolist(), start=1):
            company_name = normalize_company_name(raw_value)
            if progress_callback is not None:
                progress_callback(index, total_rows, company_name)

            if not company_name:
                enrichment_rows.append(
                    CompanyEnrichment.empty(
                        company_name="",
                        status="skipped_empty",
                        notes="The company name cell was blank.",
                    ).to_flat_dict(include_audit_columns=include_audit_columns)
                )
                continue

            cache_key = company_name.casefold()
            if cache_key not in cache:
                try:
                    cache[cache_key] = self.provider.enrich_company(company_name)
                except Exception as exc:  # pragma: no cover - external provider failure surface varies
                    cache[cache_key] = CompanyEnrichment.empty(
                        company_name=company_name,
                        status="error",
                        notes=str(exc),
                    )

            enrichment_rows.append(
                cache[cache_key].to_flat_dict(include_audit_columns=include_audit_columns)
            )

        enrichment_frame = pd.DataFrame(enrichment_rows)
        return pd.concat([dataframe.reset_index(drop=True), enrichment_frame], axis=1)
