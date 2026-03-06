import pandas as pd
import pytest

from company_gtm_enricher.csv_tools import (
    CSVValidationError,
    dataframe_to_csv_bytes,
    detect_company_column,
    load_dataframe_from_csv_bytes,
    validate_company_values,
)


def test_load_dataframe_from_csv_bytes_reads_csv() -> None:
    dataframe = load_dataframe_from_csv_bytes(b"company\nOpenAI\nPingCAP\n")
    assert list(dataframe.columns) == ["company"]
    assert dataframe.iloc[0]["company"] == "OpenAI"


def test_detect_company_column_uses_single_column_csv() -> None:
    dataframe = pd.DataFrame({"Uploaded Companies": ["OpenAI", "PingCAP"]})
    assert detect_company_column(dataframe) == "Uploaded Companies"


def test_detect_company_column_uses_common_name() -> None:
    dataframe = pd.DataFrame({"company_name": ["OpenAI"]})
    assert detect_company_column(dataframe) == "company_name"


def test_detect_company_column_raises_for_ambiguous_file() -> None:
    dataframe = pd.DataFrame({"foo": ["OpenAI"], "bar": ["PingCAP"]})
    with pytest.raises(CSVValidationError):
        detect_company_column(dataframe)


def test_validate_company_values_rejects_blank_column() -> None:
    with pytest.raises(CSVValidationError):
        validate_company_values(["", "  ", None])


def test_dataframe_to_csv_bytes_returns_utf8_csv() -> None:
    dataframe = pd.DataFrame({"company": ["OpenAI"]})
    payload = dataframe_to_csv_bytes(dataframe)
    assert payload == b"company\nOpenAI\n"
