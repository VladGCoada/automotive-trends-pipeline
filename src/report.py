from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
import json

def _pct(x: float) -> float:
    return round(float(x) * 100.0, 4)

def write_run_report(run_date: str, extract_results: list[dict] | None,
                     silver_results: list[dict], gold_results: list[dict],
                     out_dir: str = "artifacts/reports") -> str:
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    report = {
        "run_date": run_date,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "extract": extract_results or [],
        "silver": [],
        "gold": gold_results,
    }

    for r in silver_results:
        nulls = r.get("null_rates", {})
        top_nulls = sorted(nulls.items(), key=lambda kv: kv[1], reverse=True)[:8]
        report["silver"].append({
            "dataset": r.get("dataset"),
            "silver_rows": r.get("silver_rows"),
            "duplicates_removed": r.get("duplicates_removed"),
            "dropped_missing_core": r.get("dropped_missing_core", 0),
            "output": r.get("output"),
            "top_null_rates_pct": [{ "column": k, "null_pct": _pct(v) } for k,v in top_nulls],
            "note": r.get("note"),
        })

    out_path = Path(out_dir) / f"run_report_{run_date}.json"
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("\n=== RUN REPORT SUMMARY ===")
    for r in report["silver"]:
        print(f"- SILVER {r['dataset']}: rows={r['silver_rows']}, dupes_removed={r['duplicates_removed']}, dropped_missing_core={r['dropped_missing_core']}")
    for g in gold_results:
        print(f"- GOLD {g['gold_table']}: rows={g['rows']}")
    print("==========================\n")

    return str(out_path)