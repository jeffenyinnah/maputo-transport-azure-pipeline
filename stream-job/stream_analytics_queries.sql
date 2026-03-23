-- View 1: Route revenue
CREATE VIEW vw_route_revenue AS
SELECT
    JSON_VALUE(doc, '$.route_name')                               AS route_name,
    JSON_VALUE(doc, '$.vehicle_type')                             AS vehicle_type,
    JSON_VALUE(doc, '$.payment_method')                           AS payment_method,
    JSON_VALUE(doc, '$.day_of_week')                              AS day_of_week,
    CAST(JSON_VALUE(doc, '$.total_revenue_mzn')   AS FLOAT)       AS total_revenue_mzn,
    CAST(JSON_VALUE(doc, '$.passengers')          AS INT)         AS passengers,
    CAST(JSON_VALUE(doc, '$.fare_per_person_mzn') AS FLOAT)       AS fare_per_person_mzn,
    CAST(JSON_VALUE(doc, '$.occupancy_pct')       AS FLOAT)       AS occupancy_pct,
    CAST(JSON_VALUE(doc, '$.is_peak_hour')        AS BIT)         AS is_peak_hour,
    CAST(JSON_VALUE(doc, '$.hour_of_day')         AS INT)         AS hour_of_day
FROM
    OPENROWSET(
        BULK 'raw/*/*/*/*/*.json',
        DATA_SOURCE = 'MaputoLake',
        FORMAT = 'CSV',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0a'
    ) WITH (doc NVARCHAR(MAX)) AS rows

GO
-- View 2: Revenue by hour
CREATE VIEW vw_revenue_by_hour AS
SELECT
    CAST(JSON_VALUE(doc, '$.hour_of_day')        AS INT)          AS hour_of_day,
    CAST(JSON_VALUE(doc, '$.is_peak_hour')        AS BIT)         AS is_peak_hour,
    CAST(JSON_VALUE(doc, '$.total_revenue_mzn')  AS FLOAT)        AS total_revenue_mzn,
    CAST(JSON_VALUE(doc, '$.fare_per_person_mzn') AS FLOAT)       AS fare_per_person_mzn,
    CAST(JSON_VALUE(doc, '$.occupancy_pct')       AS FLOAT)       AS occupancy_pct
FROM
    OPENROWSET(
        BULK 'raw/*/*/*/*/*.json',
        DATA_SOURCE = 'MaputoLake',
        FORMAT = 'CSV',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0a'
    ) WITH (doc NVARCHAR(MAX)) AS rows

GO
-- View 3: Payment method split
CREATE VIEW vw_payment_split AS
SELECT
    JSON_VALUE(doc, '$.payment_method')                           AS payment_method,
    CAST(JSON_VALUE(doc, '$.total_revenue_mzn')  AS FLOAT)        AS total_revenue_mzn
FROM
    OPENROWSET(
        BULK 'raw/*/*/*/*/*.json',
        DATA_SOURCE = 'MaputoLake',
        FORMAT = 'CSV',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0a'
    ) WITH (doc NVARCHAR(MAX)) AS rows

GO
-- View 4: Vehicle condition impact
CREATE VIEW vw_vehicle_condition AS
SELECT
    JSON_VALUE(doc, '$.vehicle_condition')                        AS vehicle_condition,
    JSON_VALUE(doc, '$.vehicle_type')                             AS vehicle_type,
    CAST(JSON_VALUE(doc, '$.occupancy_pct')       AS FLOAT)       AS occupancy_pct,
    CAST(JSON_VALUE(doc, '$.fare_per_person_mzn') AS FLOAT)       AS fare_per_person_mzn,
    CAST(JSON_VALUE(doc, '$.total_revenue_mzn')   AS FLOAT)       AS total_revenue_mzn
FROM
    OPENROWSET(
        BULK 'raw/*/*/*/*/*.json',
        DATA_SOURCE = 'MaputoLake',
        FORMAT = 'CSV',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0a'
    ) WITH (doc NVARCHAR(MAX)) AS rows