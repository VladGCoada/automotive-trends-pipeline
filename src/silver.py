from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
import pandas as pd
import yaml

from src.validate import validate_required_columns, validate_pk_unique, null_rates

def _load_config(config_path: str) -> dict:
    return yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))

def _raw_run_dir(cfg: dict, dataset: str, run_date: str) -> Path:
    return Path(cfg["paths"]["raw"]) / dataset / f"run_date={run_date}"

def _silver_run_dir(cfg: dict, dataset: str, run_date: str) -> Path:
    p = Path(cfg["paths"].get("silver", "data/silver")) / dataset / f"run_date={run_date}"
    p.mkdir(parents=True, exist_ok=True)
    return p

def _snake(s: str) -> str:
    return (
        s.strip()
         .replace(" ", "_")
         .replace("-", "_")
         .replace("/", "_")
         .lower()
    )

def silver_nhtsa(config_path: str, run_date: str) -> dict:
    cfg = _load_config(config_path)
    raw_dir = _raw_run_dir(cfg, "nhtsa_complaints", run_date)

    import zipfile

    zip_path = raw_dir / "FLAT_CMPL.zip"

    with zipfile.ZipFile(zip_path, "r") as z:
        txt_name = "FLAT_CMPL.txt"
        with z.open(txt_name) as f:
            raw_df = pd.read_csv(
                f,
                sep="\t",
                dtype=str,
                engine="python",
                on_bad_lines="skip",
                header=None
            )

    df = pd.DataFrame({
        "odi_number": raw_df[1],           
        "received_date": raw_df[15],       
        "make": raw_df[3],
        "model": raw_df[4],
        "model_year": raw_df[5],
        "state": raw_df[13],
        "crash": raw_df[6],               
        "fire": raw_df[8],                 
        "number_of_injured": raw_df[9],
        "number_of_deaths": raw_df[10],
        "component": raw_df[11],
        "summary": raw_df[19],
    })

    df["received_date"] = pd.to_datetime(df["received_date"], format="%Y%m%d", errors="coerce").dt.date

    for c in ["model_year", "number_of_injured", "number_of_deaths"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    for c in ["crash", "fire"]:
        df[c] = df[c].astype(str).str.strip().str.upper()
        df[c] = df[c].replace({"Y": 1, "N": 0, "YES": 1, "NO": 0, "1": 1, "0": 0, "NAN": pd.NA})
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    for c in ["make", "model", "state", "component"]:
        df[c] = df[c].astype(str).str.strip().replace({"nan": pd.NA, "None": pd.NA})
        if c in ["make", "model", "state"]:
            df[c] = df[c].str.upper()

    keep = cfg["schemas"]["silver"]["nhtsa_complaints"]["required_columns"]
    validate_required_columns(df, keep, "silver_nhtsa_complaints")

    pk = cfg["schemas"]["silver"]["nhtsa_complaints"]["primary_key"]
    before = len(df)
    df = df.drop_duplicates(subset=pk, keep="first")
    removed_dupes = before - len(df)

    out_dir = _silver_run_dir(cfg, "nhtsa_complaints", run_date)
    out_file = out_dir / "nhtsa_complaints.parquet"
    df.to_parquet(out_file, index=False)

    return {
        "dataset": "nhtsa_complaints",
        "silver_rows": int(len(df)),
        "duplicates_removed": int(removed_dupes),
        "null_rates": null_rates(df),
        "output": str(out_file),
        "note": "NHTSA FLAT_CMPL parsed as tab-delimited; python engine with on_bad_lines=skip."
    }
        
            
            

  
    rename = {
        "odi_number": "odi_number",
        "received_date": "received_date",
        "crash": "crash",
        "fire": "fire",
        "num_injured": "number_of_injured",
        "number_of_injured": "number_of_injured",
        "num_deaths": "number_of_deaths",
        "number_of_deaths": "number_of_deaths",
        "make": "make",
        "model": "model",
        "model_year": "model_year",
        "state": "state",
        "component": "component",
        "summary": "summary",
    }

    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    keep = cfg["schemas"]["silver"]["nhtsa_complaints"]["required_columns"]
    existing_keep = [c for c in keep if c in df.columns]
    df = df[existing_keep].copy()

    if "received_date" in df.columns:
        df["received_date"] = pd.to_datetime(df["received_date"], errors="coerce").dt.date

    for c in ["model_year", "number_of_injured", "number_of_deaths"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    for c in ["crash", "fire"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.upper()
            df[c] = df[c].replace({"Y": 1, "N": 0, "YES": 1, "NO": 0, "1": 1, "0": 0, "NAN": pd.NA})
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    for c in ["make", "model", "state", "component"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
            df[c] = df[c].replace({"nan": pd.NA, "None": pd.NA})
            if c in ["make", "model", "state"]:
                df[c] = df[c].str.upper()

    validate_required_columns(df, keep, "silver_nhtsa_complaints")

    pk = cfg["schemas"]["silver"]["nhtsa_complaints"]["primary_key"]
    before = len(df)
    removed_dupes = 0
    if pk:
        removed_dupes = before - len(df.drop_duplicates(subset=pk, keep="first"))
        df = df.drop_duplicates(subset=pk, keep="first")

    # Save
    out_dir = _silver_run_dir(cfg, "nhtsa_complaints", run_date)
    out_file = out_dir / "nhtsa_complaints.parquet"
    df.to_parquet(out_file, index=False)

    return {
        "dataset": "nhtsa_complaints",
        "raw_rows": None,
        "silver_rows": int(len(df)),
        "duplicates_removed": int(removed_dupes),
        "null_rates": null_rates(df),
        "output": str(out_file),
    }

def silver_nrel(config_path: str, run_date: str) -> dict:
    cfg = _load_config(config_path)
    raw_dir = _raw_run_dir(cfg, "nrel_stations", run_date)
    in_file = raw_dir / "stations.json"

    obj = json.loads(in_file.read_text(encoding="utf-8"))
    stations = obj.get("fuel_stations", [])
    df = pd.json_normalize(stations)

    df.columns = [_snake(c) for c in df.columns]

    rename = {
        "id": "id",
        "station_name": "station_name",
        "fuel_type_code": "fuel_type_code",
        "state": "state",
        "city": "city",
        "zip": "zip",
        "latitude": "latitude",
        "longitude": "longitude",
        "status_code": "status_code",
        "access_code": "access_code",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    keep = cfg["schemas"]["silver"]["nrel_stations"]["required_columns"]
    existing_keep = [c for c in keep if c in df.columns]
    df = df[existing_keep].copy()

    # Clean types
    if "id" in df.columns:
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    for c in ["latitude", "longitude"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    for c in ["state", "city", "fuel_type_code", "status_code", "access_code"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
            df[c] = df[c].replace({"nan": pd.NA, "None": pd.NA})
            if c == "state":
                df[c] = df[c].str.upper()

    before = len(df)
    df = df.dropna(subset=["id", "latitude", "longitude"])
    dropped_missing_core = before - len(df)

    validate_required_columns(df, keep, "silver_nrel_stations")
    pk = cfg["schemas"]["silver"]["nrel_stations"]["primary_key"]

    before = len(df)
    removed_dupes = before - len(df.drop_duplicates(subset=pk, keep="first"))
    df = df.drop_duplicates(subset=pk, keep="first")

    out_dir = _silver_run_dir(cfg, "nrel_stations", run_date)
    out_file = out_dir / "nrel_stations.parquet"
    df.to_parquet(out_file, index=False)

    return {
        "dataset": "nrel_stations",
        "raw_rows": None,
        "silver_rows": int(len(df)),
        "duplicates_removed": int(removed_dupes),
        "dropped_missing_core": int(dropped_missing_core),
        "null_rates": null_rates(df),
        "output": str(out_file),
    }

def silver_epa(config_path: str, run_date: str) -> dict:
    cfg = _load_config(config_path)
    raw_dir = _raw_run_dir(cfg, "epa_vehicles", run_date)
    zip_path = raw_dir / "vehicles.csv.zip"

    with zipfile.ZipFile(zip_path, "r") as z:
        name = [n for n in z.namelist() if n.lower().endswith(".csv")][0]
        with z.open(name) as f:
            df = pd.read_csv(f, dtype=str, low_memory=False)

    df.columns = [_snake(c) for c in df.columns]

    rename = {
        "id": "id",
        "year": "year",
        "make": "make",
        "model": "model",
        "fueltype1": "fuel_type1",
        "fuel_type1": "fuel_type1",
        "city08": "city08",
        "highway08": "highway08",
        "comb08": "comb08",
        "co2tailpipegpm": "co2tailpipegpm",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    keep = cfg["schemas"]["silver"]["epa_vehicles"]["required_columns"]
    existing_keep = [c for c in keep if c in df.columns]
    df = df[existing_keep].copy()

    # Types
    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

    for c in ["city08", "highway08", "comb08", "co2tailpipegpm"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    for c in ["make", "model", "fuel_type1"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
            df[c] = df[c].replace({"nan": pd.NA, "None": pd.NA})
            if c in ["make", "model"]:
                df[c] = df[c].str.upper()

    before = len(df)
    df = df.dropna(subset=["id", "year", "make", "model"])
    dropped_missing_core = before - len(df)

    validate_required_columns(df, keep, "silver_epa_vehicles")
    pk = cfg["schemas"]["silver"]["epa_vehicles"]["primary_key"]

    before = len(df)
    removed_dupes = before - len(df.drop_duplicates(subset=pk, keep="first"))
    df = df.drop_duplicates(subset=pk, keep="first")

    out_dir = _silver_run_dir(cfg, "epa_vehicles", run_date)
    out_file = out_dir / "epa_vehicles.parquet"
    df.to_parquet(out_file, index=False)

    return {
        "dataset": "epa_vehicles",
        "raw_rows": None,
        "silver_rows": int(len(df)),
        "duplicates_removed": int(removed_dupes),
        "dropped_missing_core": int(dropped_missing_core),
        "null_rates": null_rates(df),
        "output": str(out_file),
    }

def run_silver_all(config_path: str, run_date: str) -> list[dict]:
    results = []
    results.append(silver_nhtsa(config_path, run_date))
    results.append(silver_nrel(config_path, run_date))
    results.append(silver_epa(config_path, run_date))
    return results