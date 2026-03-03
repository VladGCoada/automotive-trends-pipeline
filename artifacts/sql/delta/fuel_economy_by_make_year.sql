-- Delta Lake (hypothetical) for fuel_economy_by_make_year
CREATE TABLE IF NOT EXISTS fuel_economy_by_make_year (
  year BIGINT,
  make STRING,
  avg_city_mpg DOUBLE,
  avg_highway_mpg DOUBLE,
  avg_comb_mpg DOUBLE,
  avg_co2_gpm DOUBLE,
  vehicle_count BIGINT
)
USING DELTA;

MERGE INTO fuel_economy_by_make_year t
USING fuel_economy_by_make_year_staging s
ON t.year = s.year AND t.make = s.make
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
