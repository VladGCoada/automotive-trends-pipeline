# src/validate.py
from __future__ import annotations
import pandas as pd

def validate_required_columns(df: pd.DataFrame, required: list[str], table_name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"[SCHEMA] {table_name}: missing required columns: {missing}")

def validate_pk_unique(df: pd.DataFrame, pk: list[str], table_name: str) -> int:
    """
    Returns number of duplicate rows removed based on pk.
    """
    if not pk:
        return 0
    before = len(df)
    df.drop_duplicates(subset=pk, keep="first", inplace=True)
    removed = before - len(df)
    return removed

def null_rates(df: pd.DataFrame) -> dict:
    """
    Column -> null_rate in [0, 1]
    """
    if len(df) == 0:
        return {c: 0.0 for c in df.columns}
    return (df.isna().mean()).to_dict()