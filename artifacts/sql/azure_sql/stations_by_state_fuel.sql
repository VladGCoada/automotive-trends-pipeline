-- Azure SQL (hypothetical) for stations_by_state_fuel
CREATE TABLE dbo.stations_by_state_fuel (
    [state] NVARCHAR(4000),
    [fuel_type_code] NVARCHAR(4000),
    [station_count] BIGINT,
    CONSTRAINT PK_stations_by_state_fuel PRIMARY KEY ([state], [fuel_type_code])
);

MERGE INTO dbo.stations_by_state_fuel AS t
USING dbo.stations_by_state_fuel_staging AS s
ON t.[state] = s.[state] AND t.[fuel_type_code] = s.[fuel_type_code]
WHEN MATCHED THEN UPDATE SET t.[station_count] = s.[station_count]
WHEN NOT MATCHED THEN INSERT ([state], [fuel_type_code], [station_count]) VALUES (s.[state], s.[fuel_type_code], s.[station_count]);
