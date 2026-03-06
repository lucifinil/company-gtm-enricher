from __future__ import annotations

from dataclasses import dataclass, field
from threading import Event, Lock, Thread
from typing import Optional

import pandas as pd

from company_gtm_enricher.csv_tools import normalize_company_name
from company_gtm_enricher.enrichment_service import CompanyEnrichmentService, EnrichmentStopped
from company_gtm_enricher.providers.base import EnrichmentProvider


@dataclass
class EnrichmentJob:
    input_filename: str
    output_filename: str
    total_companies: int
    processed_companies: int = 0
    current_company: str = ""
    status: str = "idle"
    error_message: str = ""
    result_dataframe: Optional[pd.DataFrame] = None
    pause_event: Event = field(default_factory=Event, repr=False)
    stop_event: Event = field(default_factory=Event, repr=False)
    lock: Lock = field(default_factory=Lock, repr=False)
    thread: Optional[Thread] = field(default=None, repr=False)


def start_enrichment_job(
    dataframe: pd.DataFrame,
    company_column: str,
    provider: EnrichmentProvider,
    include_audit_columns: bool,
    batch_size: int,
    input_filename: str,
    output_filename: str,
) -> EnrichmentJob:
    job = EnrichmentJob(
        input_filename=input_filename,
        output_filename=output_filename,
        total_companies=_count_unique_companies(dataframe, company_column),
        status="running",
    )
    thread = Thread(
        target=_run_enrichment_job,
        kwargs={
            "job": job,
            "dataframe": dataframe.copy(deep=True),
            "company_column": company_column,
            "provider": provider,
            "include_audit_columns": include_audit_columns,
            "batch_size": batch_size,
        },
        daemon=True,
    )
    job.thread = thread
    thread.start()
    return job


def _count_unique_companies(dataframe: pd.DataFrame, company_column: str) -> int:
    normalized_names = [
        normalize_company_name(raw_value) for raw_value in dataframe[company_column].tolist()
    ]
    return len({company_name.casefold() for company_name in normalized_names if company_name})


def _run_enrichment_job(
    job: EnrichmentJob,
    dataframe: pd.DataFrame,
    company_column: str,
    provider: EnrichmentProvider,
    include_audit_columns: bool,
    batch_size: int,
) -> None:
    service = CompanyEnrichmentService(provider=provider)

    def on_progress(current: int, total: int, company_name: str) -> None:
        with job.lock:
            job.processed_companies = current
            job.total_companies = total
            job.current_company = company_name
            if job.status != "stopping":
                job.status = "paused" if job.pause_event.is_set() else "running"

    try:
        result_dataframe = service.enrich_dataframe(
            dataframe=dataframe,
            company_column=company_column,
            include_audit_columns=include_audit_columns,
            batch_size=batch_size,
            progress_callback=on_progress,
            should_pause=job.pause_event.is_set,
            should_stop=job.stop_event.is_set,
        )
    except EnrichmentStopped:
        with job.lock:
            job.status = "stopped"
            job.current_company = ""
        return
    except Exception as exc:
        with job.lock:
            job.status = "failed"
            job.error_message = str(exc)
            job.current_company = ""
        return

    with job.lock:
        job.status = "completed"
        job.current_company = ""
        job.processed_companies = job.total_companies
        job.result_dataframe = result_dataframe
