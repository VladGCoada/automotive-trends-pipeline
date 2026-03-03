-- Azure SQL (hypothetical) for complaints_by_make_model_year
CREATE TABLE dbo.complaints_by_make_model_year (
    [model_year] BIGINT,
    [make] NVARCHAR(4000),
    [model] NVARCHAR(4000),
    [complaint_count] NVARCHAR(4000),
    [crash_count] BIGINT,
    [fire_count] BIGINT,
    [injured_sum] BIGINT,
    [deaths_sum] BIGINT,
    CONSTRAINT PK_complaints_by_make_model_year PRIMARY KEY ([model_year], [make], [model])
);

MERGE INTO dbo.complaints_by_make_model_year AS t
USING dbo.complaints_by_make_model_year_staging AS s
ON t.[model_year] = s.[model_year] AND t.[make] = s.[make] AND t.[model] = s.[model]
WHEN MATCHED THEN UPDATE SET t.[complaint_count] = s.[complaint_count], t.[crash_count] = s.[crash_count], t.[fire_count] = s.[fire_count], t.[injured_sum] = s.[injured_sum], t.[deaths_sum] = s.[deaths_sum]
WHEN NOT MATCHED THEN INSERT ([model_year], [make], [model], [complaint_count], [crash_count], [fire_count], [injured_sum], [deaths_sum]) VALUES (s.[model_year], s.[make], s.[model], s.[complaint_count], s.[crash_count], s.[fire_count], s.[injured_sum], s.[deaths_sum]);
