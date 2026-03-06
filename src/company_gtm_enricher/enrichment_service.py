from __future__ import annotations

import time
from typing import Callable, Dict, List, Optional, Sequence

import pandas as pd

from company_gtm_enricher.csv_tools import normalize_company_name, validate_company_values
from company_gtm_enricher.models import CompanyEnrichment
from company_gtm_enricher.providers.base import EnrichmentProvider

ProgressCallback = Callable[[int, int, str], None]
StateCallback = Callable[[], bool]


class EnrichmentStopped(Exception):
    """Raised when the active enrichment run is stopped by the user."""


class CompanyEnrichmentService:
    def __init__(self, provider: EnrichmentProvider) -> None:
        self.provider = provider

    def enrich_dataframe(
        self,
        dataframe: pd.DataFrame,
        company_column: str,
        include_audit_columns: bool = True,
        batch_size: int = 1,
        progress_callback: Optional[ProgressCallback] = None,
        should_pause: Optional[StateCallback] = None,
        should_stop: Optional[StateCallback] = None,
    ) -> pd.DataFrame:
        if company_column not in dataframe.columns:
            raise ValueError(f"Column `{company_column}` does not exist in the uploaded CSV.")

        normalized_company_names = [
            normalize_company_name(raw_value) for raw_value in dataframe[company_column].tolist()
        ]
        validate_company_values(normalized_company_names)
        cache: Dict[str, CompanyEnrichment] = {}
        unique_company_names = _unique_company_names(normalized_company_names)
        total_unique_companies = len(unique_company_names)
        processed_companies = 0

        for company_batch in _chunked(unique_company_names, batch_size):
            _wait_if_paused(should_pause=should_pause, should_stop=should_stop)
            _raise_if_stopped(should_stop=should_stop)
            batch_results = self._enrich_batch(company_batch)
            for company_name in company_batch:
                cache[company_name.casefold()] = batch_results[company_name.casefold()]
                processed_companies += 1
                if progress_callback is not None:
                    progress_callback(processed_companies, total_unique_companies, company_name)

        enrichment_rows = []
        for company_name in normalized_company_names:
            if not company_name:
                enrichment_rows.append(
                    CompanyEnrichment.empty(
                        company_name="",
                        status="skipped_empty",
                        notes="The company name cell was blank.",
                    ).to_flat_dict(include_audit_columns=include_audit_columns)
                )
                continue

            enrichment_rows.append(
                cache[company_name.casefold()].to_flat_dict(include_audit_columns=include_audit_columns)
            )

        enrichment_frame = pd.DataFrame(enrichment_rows)
        return pd.concat([dataframe.reset_index(drop=True), enrichment_frame], axis=1)

    def _enrich_batch(self, company_names: Sequence[str]) -> Dict[str, CompanyEnrichment]:
        results: Dict[str, CompanyEnrichment] = {}
        batch_method = getattr(self.provider, "enrich_companies", None)

        if callable(batch_method) and len(company_names) > 1:
            try:
                batch_results = batch_method(company_names)
                for company_name in company_names:
                    enrichment = _extract_company_enrichment(
                        batch_results=batch_results,
                        company_name=company_name,
                    )
                    if enrichment is not None:
                        results[company_name.casefold()] = enrichment
            except Exception:
                pass

        for company_name in company_names:
            company_key = company_name.casefold()
            if company_key in results:
                continue
            results[company_key] = self._safe_enrich_company(company_name)

        return results

    def _safe_enrich_company(self, company_name: str) -> CompanyEnrichment:
        try:
            return self.provider.enrich_company(company_name)
        except Exception as exc:  # pragma: no cover - external provider failure surface varies
            return CompanyEnrichment.empty(
                company_name=company_name,
                status="error",
                notes=str(exc),
            )


def _chunked(company_names: Sequence[str], batch_size: int) -> List[List[str]]:
    normalized_batch_size = batch_size if batch_size > 0 else 1
    return [
        list(company_names[index : index + normalized_batch_size])
        for index in range(0, len(company_names), normalized_batch_size)
    ]


def _extract_company_enrichment(
    batch_results: object,
    company_name: str,
) -> Optional[CompanyEnrichment]:
    if not isinstance(batch_results, dict):
        return None

    direct_match = batch_results.get(company_name)
    if isinstance(direct_match, CompanyEnrichment):
        return direct_match

    lowercase_match = batch_results.get(company_name.casefold())
    if isinstance(lowercase_match, CompanyEnrichment):
        return lowercase_match

    return None


def _raise_if_stopped(should_stop: Optional[StateCallback]) -> None:
    if should_stop is not None and should_stop():
        raise EnrichmentStopped("The enrichment run was stopped.")


def _unique_company_names(company_names: Sequence[str]) -> List[str]:
    seen = set()
    unique_names = []
    for company_name in company_names:
        if not company_name:
            continue
        company_key = company_name.casefold()
        if company_key in seen:
            continue
        seen.add(company_key)
        unique_names.append(company_name)
    return unique_names


def _wait_if_paused(
    should_pause: Optional[StateCallback],
    should_stop: Optional[StateCallback],
) -> None:
    while should_pause is not None and should_pause():
        _raise_if_stopped(should_stop=should_stop)
        time.sleep(0.2)
