-- Delta Lake (hypothetical) for complaints_by_make_model_year
CREATE TABLE IF NOT EXISTS complaints_by_make_model_year (
  model_year BIGINT,
  make STRING,
  model STRING,
  complaint_count STRING,
  crash_count BIGINT,
  fire_count BIGINT,
  injured_sum BIGINT,
  deaths_sum BIGINT
)
USING DELTA;

MERGE INTO complaints_by_make_model_year t
USING complaints_by_make_model_year_staging s
ON t.model_year = s.model_year AND t.make = s.make AND t.model = s.model
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
