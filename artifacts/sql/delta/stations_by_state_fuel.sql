-- Delta Lake (hypothetical) for stations_by_state_fuel
CREATE TABLE IF NOT EXISTS stations_by_state_fuel (
  state STRING,
  fuel_type_code STRING,
  station_count BIGINT
)
USING DELTA;

MERGE INTO stations_by_state_fuel t
USING stations_by_state_fuel_staging s
ON t.state = s.state AND t.fuel_type_code = s.fuel_type_code
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
