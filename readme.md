Automotive Trends Pipeline (Raw / Silver / Gold)

How to Run the Project
Python 3.10+ .
1.Clone the repository
git clone https://github.com/VladGCoada/automotive-trends-pipeline.git
cd automotive-trends-pipeline

2.Create and activate virtual environment
Windows (PowerShell):
create :   python -m venv .venv
activare:   .venv\Scripts\Activate

3.Install dependencies
pip install -r requirements.txt

4.Set NREL API Key (required for fuel station dataset)

Windows (PowerShell):
$env:NREL_API_KEY="your_key_here"
You can obtain a free API key from:
https://developer.nrel.gov/signup/

5.Run the full pipeline
python main.py --run-date 2026-03-03

What this is

This is a local-first Python data pipeline built to explore broader automotive industry trends using public datasets.

It pulls data from:

NHTSA – Vehicle safety complaints

NREL (DOE) – Alternative fuel stations

EPA – Vehicle fuel economy

Architecture (Simple Version)
Raw → Silver → Gold
Raw

Data stored exactly as downloaded

Partitioned by run_date

Metadata saved alongside each dataset

Per dataset:

Select relevant columns

Standardize naming

Cast types

Handle missing values

Deduplicate using primary keys

Validate required columns

Calculate null rates

Output format: Parquet

Gold analytics layer ->>

complaints_by_make_model_year

stations_by_state_fuel

fuel_economy_by_make_year

vehicle_trends (joins complaints with fuel economy)

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

You can also run specific steps:

python main.py --steps silver,gold,report,load
What gets generated :::
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

Silver handles validation and quality.

Gold is small, aggregated, and ready for BI.

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

Data Engineering Project BOSCH
