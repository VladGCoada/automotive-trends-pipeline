from __future__ import annotations
import os, json
from pathlib import Path
from datetime import datetime, date, timezone
import requests
import yaml

def _run_dir(raw_root: Path, dataset: str, run_date: str) -> Path:
    p = raw_root / dataset / f"run_date={run_date}"
    p.mkdir(parents=True, exist_ok=True)
    return p

def _write_metadata(folder: Path, metadata: dict) -> None:
    (folder / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")

def _download_file(url: str, out_path: Path, headers: dict | None = None, params: dict | None = None) -> dict:
    with requests.get(url, headers=headers, params=params, stream=True, timeout=120) as r:
        r.raise_for_status()
        total = 0
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    total += len(chunk)
    return {"status_code": 200, "bytes_written": total}

def extract_all(config_path: str = "config.yaml", run_date: str | None = None) -> None:
    cfg = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    raw_root = Path(cfg["paths"]["raw"])

    run_date = run_date or date.today().isoformat()
    downloaded_at = datetime.now(timezone.utc).isoformat()

    nhtsa = cfg["sources"]["nhtsa"]
    nhtsa_dir = _run_dir(raw_root, "nhtsa_complaints", run_date)
    nhtsa_out = nhtsa_dir / nhtsa["out_file"]
    info = _download_file(nhtsa["url"], nhtsa_out)
    _write_metadata(nhtsa_dir, {"dataset": "nhtsa_complaints", "url": nhtsa["url"], "downloaded_at": downloaded_at, **info})

    nrel = cfg["sources"]["nrel"]
    api_key = os.getenv(nrel.get("api_key_env", "NREL_API_KEY"))
    if not api_key:
        raise RuntimeError("Missing NREL_API_KEY env var. Set it in PowerShell before running extract.")

    nrel_dir = _run_dir(raw_root, "nrel_stations", run_date)
    nrel_out = nrel_dir / nrel["out_file"]
    params = dict(nrel.get("params", {}))
    params["api_key"] = api_key
    info = _download_file(nrel["url"], nrel_out, params=params)
    _write_metadata(nrel_dir, {"dataset": "nrel_stations", "url": nrel["url"], "downloaded_at": downloaded_at, "params": {k:v for k,v in params.items() if k!="api_key"}, **info})

    epa = cfg["sources"]["epa"]
    epa_dir = _run_dir(raw_root, "epa_vehicles", run_date)
    epa_out = epa_dir / epa["out_file"]
    info = _download_file(epa["url"], epa_out)
    _write_metadata(epa_dir, {"dataset": "epa_vehicles", "url": epa["url"], "downloaded_at": downloaded_at, **info})

    print(f"[OK] Raw extract complete for run_date={run_date}")