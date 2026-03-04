from __future__ import annotations
from pathlib import Path
import pandas as pd
import yaml

def _load_config(config_path: str) -> dict:
    return yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))

def _silver_run_dir(cfg: dict, dataset: str, run_date: str) -> Path:
    return Path(cfg["paths"].get("silver", "data/silver")) / dataset / f"run_date={run_date}"

def _gold_run_dir(cfg: dict, dataset: str, run_date: str) -> Path:
    p = Path(cfg["paths"].get("gold", "data/gold")) / dataset / f"run_date={run_date}"
    p.mkdir(parents=True, exist_ok=True)
    return p

def build_gold(config_path: str, run_date: str) -> list[dict]:
    cfg = _load_config(config_path)

    nhtsa = pd.read_parquet(_silver_run_dir(cfg, "nhtsa_complaints", run_date) / "nhtsa_complaints.parquet")
    nrel  = pd.read_parquet(_silver_run_dir(cfg, "nrel_stations", run_date) / "nrel_stations.parquet")
    epa   = pd.read_parquet(_silver_run_dir(cfg, "epa_vehicles", run_date) / "epa_vehicles.parquet")

    outputs = []

    g1 = (
        nhtsa.groupby(["model_year", "make", "model"], dropna=False)
        .agg(
            complaint_count=("odi_number", "count"),
            crash_count=("crash", "sum"),
            fire_count=("fire", "sum"),
            injured_sum=("number_of_injured", "sum"),
            deaths_sum=("number_of_deaths", "sum"),
        )
        .reset_index()
    )
    out_dir = _gold_run_dir(cfg, "complaints_by_make_model_year", run_date)
    out_file = out_dir / "complaints_by_make_model_year.parquet"
    g1.to_parquet(out_file, index=False)
    outputs.append({"gold_table": "complaints_by_make_model_year", "rows": int(len(g1)), "output": str(out_file)})


    g2 = (
        nrel.groupby(["state", "fuel_type_code"], dropna=False)
        .agg(station_count=("id", "count"))
        .reset_index()
    )
    out_dir = _gold_run_dir(cfg, "stations_by_state_fuel", run_date)
    out_file = out_dir / "stations_by_state_fuel.parquet"
    g2.to_parquet(out_file, index=False)
    outputs.append({"gold_table": "stations_by_state_fuel", "rows": int(len(g2)), "output": str(out_file)})

    for c in ["city08", "highway08", "comb08", "co2tailpipegpm"]:
        epa[c] = pd.to_numeric(epa[c], errors="coerce")

    g3 = (
        epa.groupby(["year", "make"], dropna=False)
        .agg(
            avg_city_mpg=("city08", "mean"),
            avg_highway_mpg=("highway08", "mean"),
            avg_comb_mpg=("comb08", "mean"),
            avg_co2_gpm=("co2tailpipegpm", "mean"),
            vehicle_count=("id", "count"),
        )
        .reset_index()
    )
    out_dir = _gold_run_dir(cfg, "fuel_economy_by_make_year", run_date)
    out_file = out_dir / "fuel_economy_by_make_year.parquet"
    g3.to_parquet(out_file, index=False)
    outputs.append({"gold_table": "fuel_economy_by_make_year", "rows": int(len(g3)), "output": str(out_file)})

    epa_small = epa[["year", "make", "model", "comb08", "co2tailpipegpm"]].copy()
    epa_small = epa_small.rename(columns={"year": "model_year"})

    epa_agg = (
        epa_small.groupby(["model_year", "make", "model"], dropna=False)
        .agg(avg_comb_mpg=("comb08", "mean"), avg_co2_gpm=("co2tailpipegpm", "mean"))
        .reset_index()
    )

    nhtsa_agg = (
        nhtsa.groupby(["model_year", "make", "model"], dropna=False)
        .agg(complaint_count=("odi_number", "count"))
        .reset_index()
    )

    g4 = nhtsa_agg.merge(epa_agg, on=["model_year", "make", "model"], how="left")

    out_dir = _gold_run_dir(cfg, "vehicle_trends", run_date)
    out_file = out_dir / "vehicle_trends.parquet"
    g4.to_parquet(out_file, index=False)
    outputs.append({"gold_table": "vehicle_trends", "rows": int(len(g4)), "output": str(out_file)})

    return outputs