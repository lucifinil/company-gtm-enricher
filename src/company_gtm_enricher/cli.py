from __future__ import annotations

import argparse
from pathlib import Path

from company_gtm_enricher.config import AppConfig
from company_gtm_enricher.csv_tools import (
    CSVValidationError,
    dataframe_to_csv_bytes,
    detect_company_column,
    load_dataframe_from_csv_bytes,
)
from company_gtm_enricher.enrichment_service import CompanyEnrichmentService
from company_gtm_enricher.providers.factory import create_provider


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Enrich a CSV of company names with GTM data.")
    parser.add_argument("--input", required=True, help="Path to the input CSV file.")
    parser.add_argument("--output", required=True, help="Path to write the enriched CSV file.")
    parser.add_argument(
        "--company-column",
        default="",
        help="Optional company-name column. If omitted, the tool tries to infer it.",
    )
    parser.add_argument(
        "--provider",
        choices=("openai", "mock"),
        default="openai",
        help="Enrichment provider to use.",
    )
    parser.add_argument(
        "--model",
        default="",
        help="Optional OpenAI model override.",
    )
    parser.add_argument(
        "--no-audit-columns",
        action="store_true",
        help="Exclude confidence, source URLs, status, and notes from the output.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    config = AppConfig.from_env()
    input_path = Path(args.input)
    output_path = Path(args.output)

    try:
        dataframe = load_dataframe_from_csv_bytes(input_path.read_bytes())
        company_column = args.company_column or detect_company_column(dataframe)
        provider = create_provider(
            provider_name=args.provider,
            config=config,
            model_override=args.model or None,
        )
        service = CompanyEnrichmentService(provider=provider)
        enriched_dataframe = service.enrich_dataframe(
            dataframe=dataframe,
            company_column=company_column,
            include_audit_columns=not args.no_audit_columns,
        )
    except (CSVValidationError, OSError, ValueError) as exc:
        parser.exit(status=1, message=f"Error: {exc}\n")

    output_path.write_bytes(dataframe_to_csv_bytes(enriched_dataframe))
    print(f"Wrote enriched CSV to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
