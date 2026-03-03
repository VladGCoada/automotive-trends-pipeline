-- Azure SQL (hypothetical) for vehicle_trends
CREATE TABLE dbo.vehicle_trends (
    [model_year] BIGINT,
    [make] NVARCHAR(4000),
    [model] NVARCHAR(4000),
    [complaint_count] NVARCHAR(4000),
    [avg_comb_mpg] FLOAT,
    [avg_co2_gpm] FLOAT,
    CONSTRAINT PK_vehicle_trends PRIMARY KEY ([model_year], [make], [model])
);

MERGE INTO dbo.vehicle_trends AS t
USING dbo.vehicle_trends_staging AS s
ON t.[model_year] = s.[model_year] AND t.[make] = s.[make] AND t.[model] = s.[model]
WHEN MATCHED THEN UPDATE SET t.[complaint_count] = s.[complaint_count], t.[avg_comb_mpg] = s.[avg_comb_mpg], t.[avg_co2_gpm] = s.[avg_co2_gpm]
WHEN NOT MATCHED THEN INSERT ([model_year], [make], [model], [complaint_count], [avg_comb_mpg], [avg_co2_gpm]) VALUES (s.[model_year], s.[make], s.[model], s.[complaint_count], s.[avg_comb_mpg], s.[avg_co2_gpm]);
