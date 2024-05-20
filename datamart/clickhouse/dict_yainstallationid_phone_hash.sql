-- 1. ground table
CREATE OR REPLACE TABLE DB.dict_yainstallationid_phone_hash
(
    `installation_id` String,
    `phone` String,
)
ENGINE = SummingMergeTree
ORDER BY (installation_id, phone);

-- 2. materialized view (updates data rom now)
DROP VIEW IF EXISTS DB.dict_yainstallationid_phone_hash_mv;
CREATE MATERIALIZED VIEW DB.dict_yainstallationid_phone_hash_mv TO DB.dict_yainstallationid_phone_hash AS
SELECT
    e.installation_id,
    CONCAT('7', SUBSTRING(replace(replace(replace(replace(c.phone1, '(', ''), ')', ''), ' ', ''), '+', ''), 2, 12)) AS phone
FROM DB.raw_ya_events as e
	LEFT JOIN DB.raw_bx_crm_contact_uf as c ON lower(hex(MD5(CONCAT('+', CONCAT('7', SUBSTRING(replace(replace(replace(replace(c.phone1, '(', ''), ')', ''), ' ', ''), '+', ''), 2, 12))))))=replaceAll(replaceAll(simpleJSONExtractRaw(e.event_json, 'key'), '\"', ''), '+', '')
WHERE c.phone1 != '';

-- 3. initial data upload
INSERT INTO DB.dict_yainstallationid_phone_hash SELECT
    e.installation_id,
    CONCAT('7', SUBSTRING(replace(replace(replace(replace(c.phone1, '(', ''), ')', ''), ' ', ''), '+', ''), 2, 12)) AS phone
FROM DB.raw_ya_events as e
	LEFT JOIN DB.raw_bx_crm_contact_uf as c ON lower(hex(MD5(CONCAT('+', CONCAT('7', SUBSTRING(replace(replace(replace(replace(c.phone1, '(', ''), ')', ''), ' ', ''), '+', ''), 2, 12))))))=replaceAll(replaceAll(simpleJSONExtractRaw(e.event_json, 'key'), '\"', ''), '+', '')
WHERE c.phone1 != '';

-- 4. additional union view
DROP VIEW IF EXISTS DB.dict_yainstallationid_phone_all;
CREATE VIEW DB.dict_yainstallationid_phone_all AS
SELECT * FROM (
	SELECT
		installation_id,
		phone
	FROM DB.dict_yainstallationid_phone
UNION ALL
SELECT
		installation_id,
		phone
	FROM DB.dict_yainstallationid_phone_hash
) GROUP BY installation_id,phone;