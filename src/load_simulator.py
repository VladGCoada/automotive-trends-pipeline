from __future__ import annotations
from pathlib import Path
import pandas as pd

AZURE_SQL_TYPES = {
    "int64": "BIGINT",
    "int32": "INT",
    "float64": "FLOAT",
    "object": "NVARCHAR(4000)",
    "bool": "BIT",
}

def _azure_type(dtype) -> str:
    s = str(dtype)
    if s == "Int64":
        return "BIGINT"
    if s.startswith("float"):
        return "FLOAT"
    return "NVARCHAR(4000)"

def _delta_type(dtype) -> str:
    s = str(dtype)
    if s == "Int64":
        return "BIGINT"
    if s.startswith("float"):
        return "DOUBLE"
    return "STRING"

def _ddl_cols(df: pd.DataFrame, dialect: str) -> str:
    lines = []
    for c in df.columns:
        t = _azure_type(df[c].dtype) if dialect == "azure" else _delta_type(df[c].dtype)
        lines.append(f"    [{c}] {t}" if dialect == "azure" else f"  {c} {t}")
    return ",\n".join(lines)

def _merge_sql(table: str, keys: list[str], cols: list[str]) -> str:
    on_clause = " AND ".join([f"t.[{k}] = s.[{k}]" for k in keys])
    non_keys = [c for c in cols if c not in keys]
    set_clause = ", ".join([f"t.[{c}] = s.[{c}]" for c in non_keys])
    insert_cols = ", ".join([f"[{c}]" for c in cols])
    insert_vals = ", ".join([f"s.[{c}]" for c in cols])

    return f"""MERGE INTO dbo.{table} AS t
USING dbo.{table}_staging AS s
ON {on_clause}
WHEN MATCHED THEN UPDATE SET {set_clause}
WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
"""

def generate_gold_sql(run_date: str, gold_base: str = "data/gold", out_base: str = "artifacts/sql") -> dict:
    out_azure = Path(out_base) / "azure_sql"
    out_delta = Path(out_base) / "delta"
    out_azure.mkdir(parents=True, exist_ok=True)
    out_delta.mkdir(parents=True, exist_ok=True)

    tables = {
        "complaints_by_make_model_year": ["model_year", "make", "model"],
        "stations_by_state_fuel": ["state", "fuel_type_code"],
        "fuel_economy_by_make_year": ["year", "make"],
        "vehicle_trends": ["model_year", "make", "model"],
    }

    results = {}

    for table, keys in tables.items():
        p = Path(gold_base) / table / f"run_date={run_date}" / f"{table}.parquet"
        df = pd.read_parquet(p)
        cols = df.columns.tolist()

        ddl_azure = f"""-- Azure SQL (hypothetical) for {table}
CREATE TABLE dbo.{table} (
{_ddl_cols(df, "azure")},
    CONSTRAINT PK_{table} PRIMARY KEY ({", ".join([f"[{k}]" for k in keys])})
);
"""
        merge_azure = _merge_sql(table, keys, cols)

        (out_azure / f"{table}.sql").write_text(ddl_azure + "\n" + merge_azure, encoding="utf-8")

        ddl_delta = f"""-- Delta Lake (hypothetical) for {table}
CREATE TABLE IF NOT EXISTS {table} (
{_ddl_cols(df, "delta")}
)
USING DELTA;
"""
        merge_delta = f"""MERGE INTO {table} t
USING {table}_staging s
ON {" AND ".join([f"t.{k} = s.{k}" for k in keys])}
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
"""
        (out_delta / f"{table}.sql").write_text(ddl_delta + "\n" + merge_delta, encoding="utf-8")

        results[table] = {
            "azure_sql": str(out_azure / f"{table}.sql"),
            "delta_sql": str(out_delta / f"{table}.sql"),
        }

    return results