-- Azure SQL (hypothetical) for fuel_economy_by_make_year
CREATE TABLE dbo.fuel_economy_by_make_year (
    [year] BIGINT,
    [make] NVARCHAR(4000),
    [avg_city_mpg] FLOAT,
    [avg_highway_mpg] FLOAT,
    [avg_comb_mpg] FLOAT,
    [avg_co2_gpm] FLOAT,
    [vehicle_count] BIGINT,
    CONSTRAINT PK_fuel_economy_by_make_year PRIMARY KEY ([year], [make])
);

MERGE INTO dbo.fuel_economy_by_make_year AS t
USING dbo.fuel_economy_by_make_year_staging AS s
ON t.[year] = s.[year] AND t.[make] = s.[make]
WHEN MATCHED THEN UPDATE SET t.[avg_city_mpg] = s.[avg_city_mpg], t.[avg_highway_mpg] = s.[avg_highway_mpg], t.[avg_comb_mpg] = s.[avg_comb_mpg], t.[avg_co2_gpm] = s.[avg_co2_gpm], t.[vehicle_count] = s.[vehicle_count]
WHEN NOT MATCHED THEN INSERT ([year], [make], [avg_city_mpg], [avg_highway_mpg], [avg_comb_mpg], [avg_co2_gpm], [vehicle_count]) VALUES (s.[year], s.[make], s.[avg_city_mpg], s.[avg_highway_mpg], s.[avg_comb_mpg], s.[avg_co2_gpm], s.[vehicle_count]);
