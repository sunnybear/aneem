-- 1. ground table
CREATE TABLE DB.dict_ctphone_yclid
(
    `phone` String,
    `yclid` String
)
ENGINE = SummingMergeTree
ORDER BY (phone, yclid);

-- 2. materialized view (updates data rom now)
CREATE MATERIALIZED VIEW DB.dict_ctphone_yclid_mv TO DB.dict_ctphone_yclid AS
SELECT
	distinct callerNumber as phone,
	yaClientId as yclid
FROM DB.raw_ct_calls
WHERE yclid<>'';

-- 3. initial data upload
INSERT INTO DB.dict_ctphone_yclid SELECT
	distinct callerNumber as phone,
	yaClientId as yclid
FROM DB.raw_ct_calls
WHERE yclid<>'';