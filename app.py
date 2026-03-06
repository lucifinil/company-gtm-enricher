from __future__ import annotations

import io
import time
from pathlib import Path
from typing import Optional

import streamlit as st

from company_gtm_enricher.config import AppConfig
from company_gtm_enricher.csv_tools import (
    CSVValidationError,
    dataframe_to_csv_bytes,
    detect_company_column,
    load_dataframe_from_csv_bytes,
)
from company_gtm_enricher.job_runner import EnrichmentJob, start_enrichment_job
from company_gtm_enricher.providers.factory import create_provider


ACTIVE_JOB_STATUSES = {"running", "paused", "stopping"}


def main() -> None:
    config = AppConfig.from_env()
    job = st.session_state.get("enrichment_job")

    st.set_page_config(page_title="Company GTM Enricher", page_icon=":bar_chart:")
    st.title("Company GTM Enricher")
    st.caption(
        "Upload a CSV of company names, enrich public GTM fields, and download a refreshed CSV."
    )

    with st.sidebar:
        st.header("Run Settings")
        provider_name = st.selectbox(
            "Provider",
            options=["openai", "mock"],
            help="Use mock to test the workflow without live API calls.",
        )
        model_name = st.text_input("OpenAI model", value=config.openai_model)
        batch_size = st.number_input(
            "Batch size",
            min_value=1,
            value=config.openai_batch_size,
            step=1,
            help="Unique companies are grouped into batches of this size before sending requests.",
        )
        include_audit_columns = st.checkbox(
            "Include audit columns",
            value=True,
            help="Adds enrichment confidence, source URLs, status, and notes.",
        )

        st.markdown("**Environment**")
        st.write(
            "Max companies per run: "
            + (f"`{config.max_companies_per_run}`" if config.max_companies_per_run else "`unlimited`")
        )
        st.write(
            "OpenAI API key loaded: "
            + ("`yes`" if bool(config.openai_api_key) else "`no`")
        )

    _render_job_controls(job)

    uploaded_file = st.file_uploader("Upload a CSV", type=["csv"])
    if uploaded_file is None:
        st.info("Upload a CSV file to begin.")
        _schedule_refresh(job)
        return

    try:
        dataframe = load_dataframe_from_csv_bytes(uploaded_file.getvalue())
    except CSVValidationError as exc:
        st.error(str(exc))
        return

    if len(dataframe.index) == 0:
        st.error("The CSV file is empty.")
        return

    if config.max_companies_per_run and len(dataframe.index) > config.max_companies_per_run:
        st.error(
            "This file has "
            f"{len(dataframe.index)} rows, which exceeds the configured limit of "
            f"{config.max_companies_per_run}."
        )
        return

    try:
        guessed_company_column = detect_company_column(dataframe)
    except CSVValidationError:
        guessed_company_column = str(dataframe.columns[0])
    company_column = st.selectbox(
        "Company name column",
        options=list(dataframe.columns),
        index=list(dataframe.columns).index(guessed_company_column),
    )

    st.subheader("Preview")
    st.dataframe(dataframe.head(10), width="stretch")

    if not _is_job_active(job):
        if not st.button("Enrich CSV", type="primary"):
            _render_job_result(job)
            return

        try:
            provider = create_provider(
                provider_name=provider_name,
                config=config,
                model_override=model_name.strip() or None,
            )
        except ValueError as exc:
            st.error(str(exc))
            return

        st.session_state["enrichment_job"] = start_enrichment_job(
            dataframe=dataframe,
            company_column=company_column,
            provider=provider,
            include_audit_columns=include_audit_columns,
            batch_size=int(batch_size),
            input_filename=uploaded_file.name,
            output_filename=uploaded_file.name.rsplit(".", 1)[0] + "_enriched.csv",
        )
        st.rerun()

    _render_job_result(st.session_state.get("enrichment_job"))
    _schedule_refresh(st.session_state.get("enrichment_job"))


def _render_job_controls(job: Optional[EnrichmentJob]) -> None:
    if job is None:
        return

    with job.lock:
        status = job.status
        total_companies = job.total_companies
        processed_companies = job.processed_companies
        current_company = job.current_company
        error_message = job.error_message
        started_at = job.started_at
        finished_at = job.finished_at
        backup_count = job.backup_count
        last_backup_path = job.last_backup_path
        last_backup_at = job.last_backup_at

    st.subheader("Run Status")
    st.write(f"Status: `{status}`")
    st.write(f"Processed companies: `{processed_companies}` / `{total_companies}`")
    st.write(f"Current company: `{current_company or 'waiting'}`")
    st.write(f"Elapsed time: `{_format_duration((finished_at or time.time()) - started_at)}`")
    st.write(f"Interim backups written: `{backup_count}`")
    if last_backup_at is not None and last_backup_path:
        st.write(
            "Latest backup: "
            + f"`{last_backup_path}` at `{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_backup_at))}`"
        )
        backup_file = Path(last_backup_path)
        if backup_file.exists():
            st.download_button(
                label="Download latest backup CSV",
                data=backup_file.read_bytes(),
                file_name=backup_file.name,
                mime="text/csv",
            )

    progress_total = total_companies or 1
    progress_value = min(processed_companies / progress_total, 1.0)
    progress_label = (
        f"Processing {processed_companies}/{total_companies}: {current_company or 'waiting'}"
        if status in ACTIVE_JOB_STATUSES
        else f"{status.capitalize()} {processed_companies}/{total_companies}"
    )
    st.progress(progress_value, text=progress_label)

    controls = st.columns(3)
    if status == "running" and controls[0].button("Pause"):
        job.pause_event.set()
        with job.lock:
            job.status = "paused"
        st.rerun()
    if status == "paused" and controls[0].button("Resume"):
        job.pause_event.clear()
        with job.lock:
            job.status = "running"
        st.rerun()
    if status in {"running", "paused"} and controls[1].button("Stop"):
        job.stop_event.set()
        job.pause_event.clear()
        with job.lock:
            job.status = "stopping"
        st.rerun()
    if status in {"completed", "failed", "stopped"} and controls[2].button("Clear run state"):
        st.session_state.pop("enrichment_job", None)
        st.rerun()

    if status == "failed" and error_message:
        st.error(error_message)
    if status == "stopped":
        st.warning("The enrichment run was stopped before completion.")


def _render_job_result(job: Optional[EnrichmentJob]) -> None:
    if job is None:
        return

    with job.lock:
        status = job.status
        result_dataframe = job.result_dataframe
        output_filename = job.output_filename

    if status != "completed" or result_dataframe is None:
        return

    st.success("Enrichment complete.")
    st.subheader("Enriched Preview")
    st.dataframe(result_dataframe.head(25), width="stretch")

    csv_bytes = dataframe_to_csv_bytes(result_dataframe)
    st.download_button(
        label="Download enriched CSV",
        data=io.BytesIO(csv_bytes),
        file_name=output_filename,
        mime="text/csv",
    )


def _is_job_active(job: Optional[EnrichmentJob]) -> bool:
    if job is None:
        return False
    with job.lock:
        return job.status in ACTIVE_JOB_STATUSES


def _schedule_refresh(job: Optional[EnrichmentJob]) -> None:
    if not _is_job_active(job):
        return
    time.sleep(1)
    st.rerun()


def _format_duration(duration_seconds: float) -> str:
    total_seconds = max(int(duration_seconds), 0)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


if __name__ == "__main__":
    main()
