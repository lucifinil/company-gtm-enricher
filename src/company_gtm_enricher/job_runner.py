from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Dict, Optional

import pandas as pd

from company_gtm_enricher.csv_tools import dataframe_to_csv_bytes, normalize_company_name
from company_gtm_enricher.enrichment_service import CompanyEnrichmentService, EnrichmentStopped
from company_gtm_enricher.models import CompanyEnrichment
from company_gtm_enricher.providers.base import EnrichmentProvider


INTERIM_BACKUP_INTERVAL_SECONDS = 600
BACKUP_DIRECTORY_NAME = "backups"


@dataclass
class EnrichmentJob:
    input_filename: str
    output_filename: str
    total_companies: int
    started_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None
    processed_companies: int = 0
    current_company: str = ""
    status: str = "idle"
    error_message: str = ""
    result_dataframe: Optional[pd.DataFrame] = None
    backup_count: int = 0
    last_backup_path: str = ""
    last_backup_at: Optional[float] = None
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
    backup_directory = Path.cwd() / BACKUP_DIRECTORY_NAME
    backup_directory.mkdir(parents=True, exist_ok=True)
    next_backup_at = job.started_at + INTERIM_BACKUP_INTERVAL_SECONDS
    partial_cache: Dict[str, CompanyEnrichment] = {}

    def on_progress(current: int, total: int, company_name: str) -> None:
        with job.lock:
            job.processed_companies = current
            job.total_companies = total
            job.current_company = company_name
            if job.status != "stopping":
                job.status = "paused" if job.pause_event.is_set() else "running"

    def on_batch_complete(cache: Dict[str, CompanyEnrichment]) -> None:
        nonlocal next_backup_at, partial_cache
        partial_cache = dict(cache)
        now = time.time()
        if now < next_backup_at or not partial_cache:
            return
        backup_path = _write_backup_file(
            service=service,
            dataframe=dataframe,
            company_column=company_column,
            include_audit_columns=include_audit_columns,
            cache=partial_cache,
            backup_directory=backup_directory,
            input_filename=job.input_filename,
        )
        with job.lock:
            job.backup_count += 1
            job.last_backup_at = now
            job.last_backup_path = str(backup_path)
        while next_backup_at <= now:
            next_backup_at += INTERIM_BACKUP_INTERVAL_SECONDS

    try:
        result_dataframe = service.enrich_dataframe(
            dataframe=dataframe,
            company_column=company_column,
            include_audit_columns=include_audit_columns,
            batch_size=batch_size,
            progress_callback=on_progress,
            batch_complete_callback=on_batch_complete,
            should_pause=job.pause_event.is_set,
            should_stop=job.stop_event.is_set,
        )
    except EnrichmentStopped:
        _flush_partial_backup_if_needed(
            job=job,
            service=service,
            dataframe=dataframe,
            company_column=company_column,
            include_audit_columns=include_audit_columns,
            partial_cache=partial_cache,
            backup_directory=backup_directory,
            reason="stopped",
        )
        with job.lock:
            job.status = "stopped"
            job.current_company = ""
            job.finished_at = time.time()
        return
    except Exception as exc:
        _flush_partial_backup_if_needed(
            job=job,
            service=service,
            dataframe=dataframe,
            company_column=company_column,
            include_audit_columns=include_audit_columns,
            partial_cache=partial_cache,
            backup_directory=backup_directory,
            reason="failed",
        )
        with job.lock:
            job.status = "failed"
            job.error_message = str(exc)
            job.current_company = ""
            job.finished_at = time.time()
        return

    with job.lock:
        job.status = "completed"
        job.current_company = ""
        job.processed_companies = job.total_companies
        job.result_dataframe = result_dataframe
        job.finished_at = time.time()


def _flush_partial_backup_if_needed(
    job: EnrichmentJob,
    service: CompanyEnrichmentService,
    dataframe: pd.DataFrame,
    company_column: str,
    include_audit_columns: bool,
    partial_cache: Dict[str, CompanyEnrichment],
    backup_directory: Path,
    reason: str,
) -> None:
    if not partial_cache:
        return
    backup_path = _write_backup_file(
        service=service,
        dataframe=dataframe,
        company_column=company_column,
        include_audit_columns=include_audit_columns,
        cache=partial_cache,
        backup_directory=backup_directory,
        input_filename=job.input_filename,
        pending_notes=f"The run ended before this company was processed ({reason}).",
    )
    with job.lock:
        job.backup_count += 1
        job.last_backup_at = time.time()
        job.last_backup_path = str(backup_path)


def _write_backup_file(
    service: CompanyEnrichmentService,
    dataframe: pd.DataFrame,
    company_column: str,
    include_audit_columns: bool,
    cache: Dict[str, CompanyEnrichment],
    backup_directory: Path,
    input_filename: str,
    pending_notes: str = "This company has not been processed yet.",
) -> Path:
    partial_dataframe = service.build_dataframe_from_cache(
        dataframe=dataframe,
        company_column=company_column,
        cache=cache,
        include_audit_columns=include_audit_columns,
        pending_status="pending",
        pending_notes=pending_notes,
    )
    backup_filename = (
        f"{Path(input_filename).stem}_backup_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    )
    backup_path = backup_directory / backup_filename
    backup_path.write_bytes(dataframe_to_csv_bytes(partial_dataframe))
    return backup_path
