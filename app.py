from __future__ import annotations

import io

import streamlit as st

from company_gtm_enricher.config import AppConfig
from company_gtm_enricher.csv_tools import (
    CSVValidationError,
    dataframe_to_csv_bytes,
    detect_company_column,
    load_dataframe_from_csv_bytes,
)
from company_gtm_enricher.enrichment_service import CompanyEnrichmentService
from company_gtm_enricher.providers.factory import create_provider


def main() -> None:
    config = AppConfig.from_env()

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
        include_audit_columns = st.checkbox(
            "Include audit columns",
            value=True,
            help="Adds enrichment confidence, source URLs, status, and notes.",
        )

        st.markdown("**Environment**")
        st.write(f"Max companies per run: `{config.max_companies_per_run}`")
        st.write(
            "OpenAI API key loaded: "
            + ("`yes`" if bool(config.openai_api_key) else "`no`")
        )

    uploaded_file = st.file_uploader("Upload a CSV", type=["csv"])
    if uploaded_file is None:
        st.info("Upload a CSV file to begin.")
        return

    try:
        dataframe = load_dataframe_from_csv_bytes(uploaded_file.getvalue())
    except CSVValidationError as exc:
        st.error(str(exc))
        return

    if len(dataframe.index) == 0:
        st.error("The CSV file is empty.")
        return

    if len(dataframe.index) > config.max_companies_per_run:
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
    st.dataframe(dataframe.head(10), use_container_width=True)

    if not st.button("Enrich CSV", type="primary"):
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

    service = CompanyEnrichmentService(provider=provider)
    progress_bar = st.progress(0.0, text="Starting enrichment...")
    status_placeholder = st.empty()

    def on_progress(current: int, total: int, company_name: str) -> None:
        progress_bar.progress(
            current / total,
            text=f"Processing {current}/{total}: {company_name or 'blank row'}",
        )
        status_placeholder.caption(f"Current company: `{company_name or 'blank row'}`")

    with st.spinner("Researching companies..."):
        enriched_dataframe = service.enrich_dataframe(
            dataframe=dataframe,
            company_column=company_column,
            include_audit_columns=include_audit_columns,
            progress_callback=on_progress,
        )

    progress_bar.progress(1.0, text="Enrichment complete.")
    st.success("Enrichment complete.")
    st.subheader("Enriched Preview")
    st.dataframe(enriched_dataframe.head(25), use_container_width=True)

    csv_bytes = dataframe_to_csv_bytes(enriched_dataframe)
    output_name = uploaded_file.name.rsplit(".", 1)[0] + "_enriched.csv"
    st.download_button(
        label="Download enriched CSV",
        data=io.BytesIO(csv_bytes),
        file_name=output_name,
        mime="text/csv",
    )


if __name__ == "__main__":
    main()
