-- Delta Lake (hypothetical) for vehicle_trends
CREATE TABLE IF NOT EXISTS vehicle_trends (
  model_year BIGINT,
  make STRING,
  model STRING,
  complaint_count STRING,
  avg_comb_mpg DOUBLE,
  avg_co2_gpm DOUBLE
)
USING DELTA;

MERGE INTO vehicle_trends t
USING vehicle_trends_staging s
ON t.model_year = s.model_year AND t.make = s.make AND t.model = s.model
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
