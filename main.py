from __future__ import annotations
import argparse
from datetime import date

from src.extract import extract_all
from src.silver import run_silver_all
from src.gold import build_gold
from src.report import write_run_report
from src.load_simulator import generate_gold_sql

def parse_args():
    p = argparse.ArgumentParser(description="Local-first medallion pipeline (raw/silver/gold)")
    p.add_argument("--config", default="config.yaml")
    p.add_argument("--run-date", default=date.today().isoformat())
    p.add_argument("--steps", default="extract,silver,gold,report,load",
                   help="Comma-separated: extract,silver,gold,report,load")
    return p.parse_args()

def main():
    args = parse_args()
    steps = [s.strip().lower() for s in args.steps.split(",") if s.strip()]

    extract_res = None
    silver_res = None
    gold_res = None

    if "extract" in steps:
        extract_all(config_path=args.config, run_date=args.run_date)

    if "silver" in steps:
        silver_res = run_silver_all(args.config, args.run_date)

    if "gold" in steps:
        gold_res = build_gold(args.config, args.run_date)

    if "load" in steps:
        sql_paths = generate_gold_sql(args.run_date)
        print("[OK] Generated simulated load SQL:")
        for t, paths in sql_paths.items():
            print(f"  - {t}: {paths}")

    if "report" in steps:
        silver_res = silver_res or run_silver_all(args.config, args.run_date)
        gold_res = gold_res or build_gold(args.config, args.run_date)
        rp = write_run_report(args.run_date, extract_res, silver_res, gold_res)
        print(f"[OK] Wrote run report: {rp}")

if __name__ == "__main__":
    main()