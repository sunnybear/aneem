-- 1. ground table
CREATE OR REPLACE TABLE DB.dict_yainstallationid_phone
(
    `installation_id` String,
    `phone` String,
)
ENGINE = SummingMergeTree
ORDER BY (installation_id, phone);

-- 2. materialized view (updates data rom now)
DROP VIEW IF EXISTS DB.dict_yainstallationid_phone_mv;
CREATE MATERIALIZED VIEW DB.dict_yainstallationid_phone_mv TO DB.dict_yainstallationid_phone AS
SELECT
    installation_id,
    replaceAll(replaceAll(simpleJSONExtractRaw(event_json, 'phone'), '\"', ''), '+', '') AS phone
FROM DB.raw_ya_events
WHERE phone != '';

-- 3. initial data upload
INSERT INTO DB.dict_yainstallationid_phone SELECT
    installation_id,
    replaceAll(replaceAll(simpleJSONExtractRaw(event_json, 'phone'), '\"', ''), '+', '') AS phone
FROM DB.raw_ya_events
WHERE phone != '';