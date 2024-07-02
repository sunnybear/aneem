-- 1. ground table
CREATE TABLE DB.dict_bxleadid_phone
(
    `ID` Int64,
    `phone` String,
)
ENGINE = SummingMergeTree
ORDER BY (ID, phone);

-- 2. materialized view (updates data rom now)
CREATE MATERIALIZED VIEW DB.dict_bxleadid_phone_mv TO DB.dict_bxleadid_phone AS
SELECT
    ID,
    CONCAT('7', SUBSTRING(replace(replace(replace(replace(replace(phone1, '(', ''), ')', ''), ' ', ''), '+', ''), '-', ''), 2, 12)) AS phone
FROM DB.raw_bx_crm_lead_uf;

-- 3. initial data upload
INSERT INTO DB.dict_bxleadid_phone SELECT
    ID,
    CONCAT('7', SUBSTRING(replace(replace(replace(replace(replace(phone1, '(', ''), ')', ''), ' ', ''), '+', ''), '-', ''), 2, 12)) AS phone
FROM DB.raw_bx_crm_lead_uf;