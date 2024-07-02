-- 1. ground table
CREATE TABLE DB.dict_bxdealid_phone
(
    `ID` Int64,
    `phone` String,
)
ENGINE = SummingMergeTree
ORDER BY (ID, phone);

-- 2. materialized view (updates data rom now)
CREATE MATERIALIZED VIEW DB.dict_bxdealid_phone_mv TO DB.dict_bxdealid_phone AS
SELECT
    d.ID,
    CONCAT('7', SUBSTRING(replace(replace(replace(replace(replace(phone1, '(', ''), ')', ''), ' ', ''), '+', ''), '-', ''), 2, 12)) AS phone
FROM DB.raw_bx_crm_deal as d
    LEFT JOIN DB.raw_bx_crm_contact_uf as c ON d.CONTACT_ID=c.ID;

-- 3. initial data upload
INSERT INTO DB.dict_bxdealid_phone SELECT
    d.ID,
    CONCAT('7', SUBSTRING(replace(replace(replace(replace(replace(phone1, '(', ''), ')', ''), ' ', ''), '+', ''), '-', ''), 2, 12)) AS phone
FROM DB.raw_bx_crm_deal as d
    LEFT JOIN DB.raw_bx_crm_contact_uf as c ON d.CONTACT_ID=c.ID;