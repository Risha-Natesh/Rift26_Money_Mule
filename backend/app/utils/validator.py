from __future__ import annotations

import csv
import io
from typing import Iterable

import pandas as pd

REQUIRED_COLUMNS = ["transaction_id", "sender_id", "receiver_id", "amount", "timestamp"]
ALLOWED_MIME_TYPES = {
    "text/csv",
    "application/csv",
    "application/vnd.ms-excel",
    "application/octet-stream",
    "text/plain",
}
MAX_UPLOAD_BYTES = 10 * 1024 * 1024


class CSVValidationError(ValueError):
    """Raised when uploaded CSV fails strict validation."""


def _normalize_columns(columns: Iterable[object]) -> list[str]:
    return [str(col).strip().replace("\ufeff", "") for col in columns]


def _unwrap_line_value(value: str) -> str:
    cleaned = str(value).strip().replace("\ufeff", "")
    if len(cleaned) >= 2 and cleaned.startswith('"') and cleaned.endswith('"'):
        return cleaned[1:-1]
    return cleaned


def _split_line(line: str, delimiter: str) -> list[str]:
    reader = csv.reader([line], delimiter=delimiter, quotechar='"')
    parsed = next(reader, [])
    return [str(cell).strip() for cell in parsed]


def _expand_single_column_df(raw_df: pd.DataFrame, delimiter: str) -> pd.DataFrame:
    if raw_df.shape[1] != 1:
        return raw_df

    first_column_name = str(raw_df.columns[0])
    header_line = _unwrap_line_value(first_column_name)
    if delimiter not in header_line:
        return raw_df

    header = _split_line(header_line, delimiter)
    if len(header) < 2:
        return raw_df

    rows: list[list[str]] = []
    for value in raw_df.iloc[:, 0].tolist():
        row_line = _unwrap_line_value(str(value))
        cells = _split_line(row_line, delimiter)
        if len(cells) == len(header):
            rows.append(cells)
        else:
            rows.append(cells + [""] * (len(header) - len(cells)))

    return pd.DataFrame(rows, columns=header)


def _read_candidate(text: str, delimiter: str | None) -> pd.DataFrame:
    if delimiter is None:
        parsed = pd.read_csv(
            io.StringIO(text),
            dtype=str,
            keep_default_na=False,
            sep=None,
            engine="python",
            skipinitialspace=True,
            quotechar='"',
        )
        if parsed.shape[1] == 1:
            header = str(parsed.columns[0])
            for candidate in (",", ";", "\t", "|"):
                if candidate in header:
                    return _expand_single_column_df(parsed, candidate)
        return parsed

    parsed = pd.read_csv(
        io.StringIO(text),
        dtype=str,
        keep_default_na=False,
        sep=delimiter,
        engine="python",
        skipinitialspace=True,
        quotechar='"',
    )
    return _expand_single_column_df(parsed, delimiter)


def _read_csv_robust(payload: bytes) -> pd.DataFrame:
    text = payload.decode("utf-8-sig", errors="replace")

    attempts: list[str | None] = [None, ",", ";", "\t", "|"]
    last_error: Exception | None = None

    for delimiter in attempts:
        try:
            frame = _read_candidate(text, delimiter)
            if frame.shape[1] >= 1:
                return frame
        except Exception as exc:
            last_error = exc

    raise CSVValidationError(f"Failed to parse CSV: {last_error}")


def validate_upload_metadata(filename: str | None, content_type: str | None, payload: bytes) -> None:
    if not payload:
        raise CSVValidationError("Uploaded file is empty.")

    if len(payload) > MAX_UPLOAD_BYTES:
        raise CSVValidationError(
            f"File too large. Max supported size is {MAX_UPLOAD_BYTES} bytes."
        )

    if filename and not filename.lower().endswith(".csv"):
        raise CSVValidationError("Only .csv files are allowed.")

    if content_type:
        mime = content_type.split(";")[0].strip().lower()
        if mime and mime not in ALLOWED_MIME_TYPES:
            raise CSVValidationError(
                f"Unsupported content type '{mime}'. Allowed types: {sorted(ALLOWED_MIME_TYPES)}."
            )


def validate_and_parse_csv(payload: bytes) -> pd.DataFrame:
    raw_df = _read_csv_robust(payload)

    columns = _normalize_columns(raw_df.columns)
    if len(columns) != len(set(columns)):
        raise CSVValidationError("Duplicate column names found in CSV header.")

    missing = [col for col in REQUIRED_COLUMNS if col not in columns]
    extra = [col for col in columns if col not in REQUIRED_COLUMNS]
    if missing or extra or len(columns) != len(REQUIRED_COLUMNS):
        detail = {
            "missing_columns": missing,
            "extra_columns": extra,
            "required_columns": REQUIRED_COLUMNS,
        }
        raise CSVValidationError(f"CSV columns must match exactly: {detail}")

    raw_df.columns = columns
    df = raw_df[REQUIRED_COLUMNS].copy()

    for col in ("transaction_id", "sender_id", "receiver_id", "timestamp"):
        df[col] = df[col].astype(str).map(_unwrap_line_value).str.strip()
        if (df[col] == "").any():
            raise CSVValidationError(f"Column '{col}' contains empty values.")

    df["amount"] = df["amount"].astype(str).map(_unwrap_line_value).str.strip()
    try:
        df["amount"] = pd.to_numeric(df["amount"], errors="raise")
    except Exception as exc:
        raise CSVValidationError(f"Column 'amount' must be numeric: {exc}") from exc

    if df["amount"].isna().any():
        raise CSVValidationError("Column 'amount' contains invalid numeric values.")

    try:
        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            format="%Y-%m-%d %H:%M:%S",
            errors="raise",
        )
    except Exception as exc:
        raise CSVValidationError(
            f"Column 'timestamp' must follow YYYY-MM-DD HH:MM:SS format: {exc}"
        ) from exc

    duplicates = (
        df.loc[df["transaction_id"].duplicated(keep=False), "transaction_id"]
        .sort_values()
        .unique()
        .tolist()
    )
    if duplicates:
        raise CSVValidationError(
            f"Duplicate transaction_id values detected: {duplicates[:5]}."
        )

    return df
