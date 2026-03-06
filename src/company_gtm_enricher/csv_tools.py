from __future__ import annotations

import io
from typing import Iterable

import pandas as pd


COMMON_COMPANY_COLUMNS = ("company", "company_name", "name", "account")


class CSVValidationError(ValueError):
    """Raised when the uploaded CSV cannot be processed."""


def load_dataframe_from_csv_bytes(csv_bytes: bytes) -> pd.DataFrame:
    if not csv_bytes:
        raise CSVValidationError("The uploaded file is empty.")

    try:
        dataframe = pd.read_csv(io.BytesIO(csv_bytes))
    except Exception as exc:  # pragma: no cover - pandas error surface varies
        raise CSVValidationError(f"Unable to read CSV: {exc}") from exc

    dataframe.columns = [str(column).strip() for column in dataframe.columns]
    return dataframe


def detect_company_column(dataframe: pd.DataFrame) -> str:
    if len(dataframe.columns) == 1:
        return str(dataframe.columns[0])

    normalized = {str(column).strip().lower(): str(column) for column in dataframe.columns}
    for candidate in COMMON_COMPANY_COLUMNS:
        if candidate in normalized:
            return normalized[candidate]

    raise CSVValidationError(
        "Unable to infer the company-name column. Rename it to a common value such as "
        "`company` or choose the right column in the app."
    )


def validate_company_values(company_names: Iterable[object]) -> None:
    non_blank_names = [normalize_company_name(value) for value in company_names]
    non_blank_names = [value for value in non_blank_names if value]
    if not non_blank_names:
        raise CSVValidationError("The selected company column does not contain any non-empty values.")


def dataframe_to_csv_bytes(dataframe: pd.DataFrame) -> bytes:
    return dataframe.to_csv(index=False).encode("utf-8")


def normalize_company_name(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    normalized = str(value).strip()
    if normalized.casefold() in {"", "none", "nan"}:
        return ""
    return normalized
