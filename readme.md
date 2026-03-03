Automotive Trends Pipeline (Raw / Silver / Gold)
What this is

This is a local-first Python data pipeline built to explore broader automotive industry trends using public datasets.

It pulls data from:

NHTSA – Vehicle safety complaints

NREL (DOE) – Alternative fuel stations

EPA – Vehicle fuel economy

The goal wasn’t to overengineer it, but to build something clean, understandable, and structured the way a real data platform would be — using a simple medallion architecture.

Architecture (Simple Version)
Raw → Silver → Gold
Raw

Data stored exactly as downloaded

Partitioned by run_date

Never modified

Metadata saved alongside each dataset

Why?
So the pipeline is reproducible and safe to reprocess.

Silver

This is where cleaning happens.

Per dataset:

Select relevant columns

Standardize naming

Cast types

Handle missing values

Deduplicate using primary keys

Validate required columns

Calculate null rates

Output format: Parquet

This layer is structured and reliable.

Gold

This is the analytics layer.

I created:

complaints_by_make_model_year

stations_by_state_fuel

fuel_economy_by_make_year

vehicle_trends (joins complaints with fuel economy)

These tables are ready for dashboards or reporting.

Data Sources
NHTSA Complaints

Bulk flat file:
https://static.nhtsa.gov/odi/ffdd/cmpl/FLAT_CMPL.zip

Tab-delimited file inside ZIP

Parsed using Python engine (some malformed rows are skipped)

Deduplicated by complaint ID

NREL Alternative Fuel Stations

API endpoint:
https://developer.nrel.gov/api/alt-fuel-stations/v1.json

Requires:

NREL_API_KEY

Retrieved full dataset using limit=all.

EPA Fuel Economy

Bulk CSV:
https://www.fueleconomy.gov/feg/epadata/vehicles.csv.zip

Selected MPG and CO2-related fields for analysis.

How to Run It

1. Create venv
   python -m venv .venv
   .venv\Scripts\Activate
2. Install deps
   pip install -r requirements.txt
3. Set NREL API key
   $env:NREL_API_KEY="your_key_here"
4. Run everything
   python main.py --run-date 2026-03-03

You can also run specific steps:

python main.py --steps silver,gold,report,load
What gets generated
Clean datasets
data/silver/
data/gold/
Run report
artifacts/reports/run*report*<date>.json

Includes:

Row counts

Duplicates removed

Null rates

Gold table sizes

Simulated load scripts
artifacts/sql/azure_sql/
artifacts/sql/delta/

For each gold table:

Azure SQL DDL + MERGE

Delta Lake DDL + MERGE

These demonstrate how the gold layer would be loaded in a real environment.

Why I built it this way

Medallion structure keeps things clean and easy to reason about.

Raw is immutable so I can reprocess safely.

Silver handles validation and quality.

Gold is small, aggregated, and ready for BI.

Load is simulated because the assessment doesn’t require a live database.

I kept it intentionally mid-level — readable, modular, and easy to explain.

Automation idea

I would schedule this daily.

Reason:

NHTSA complaints update frequently.

NREL stations change occasionally.

EPA data is mostly stable.

Could be automated with:

Azure Data Factory

GitHub Actions (cron)

Azure DevOps pipeline

Project Structure
src/
extract.py
silver.py
gold.py
validate.py
load_simulator.py
report.py

data/
raw/
silver/
gold/

artifacts/
reports/
sql/
Author

Vlad Coada
Data Engineering Project
