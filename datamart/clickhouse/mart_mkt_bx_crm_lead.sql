-- 1. ground table
CREATE OR REPLACE TABLE DB.mart_mkt_bx_crm_lead
(
	`ID` Int32,
    `TITLE` String,
    `HONORIFIC` String,
    `NAME` String,
    `SECOND_NAME` String,
    `LAST_NAME` String,
    `COMPANY_TITLE` String,
    `COMPANY_ID` Int32,
    `CONTACT_ID` String,
    `IS_RETURN_CUSTOMER` String,
    `BIRTHDATE` String,
    `SOURCE_ID` String,
    `SOURCE_DESCRIPTION` String,
    `STATUS_ID` String,
    `STATUS_DESCRIPTION` String,
    `POST` String,
    `COMMENTS` String,
    `CURRENCY_ID` String,
    `OPPORTUNITY` Float32,
    `IS_MANUAL_OPPORTUNITY` String,
    `HAS_PHONE` String,
    `HAS_EMAIL` String,
    `HAS_IMOL` String,
    `ASSIGNED_BY_ID` Int32,
    `CREATED_BY_ID` Int32,
    `MODIFY_BY_ID` Int32,
    `DATE_CREATE` DateTime,
    `DATE_MODIFY` DateTime,
    `DATE_CLOSED` DateTime,
    `STATUS_SEMANTIC_ID` String,
    `OPENED` String,
    `ORIGINATOR_ID` String,
    `ORIGIN_ID` String,
    `MOVED_BY_ID` Int32,
    `MOVED_TIME` DateTime,
    `ADDRESS` String,
    `ADDRESS_2` String,
    `ADDRESS_CITY` String,
    `ADDRESS_POSTAL_CODE` String,
    `ADDRESS_REGION` String,
    `ADDRESS_PROVINCE` String,
    `ADDRESS_COUNTRY` String,
    `ADDRESS_COUNTRY_CODE` Int32,
    `ADDRESS_LOC_ADDR_ID` Int32,
    `UTM_SOURCE` String,
    `UTM_MEDIUM` String,
    `UTM_CAMPAIGN` String,
    `UTM_CONTENT` String,
    `UTM_TERM` String,
    `LAST_ACTIVITY_BY` Int32,
    `LAST_ACTIVITY_TIME` DateTime,
    `s.ID` Nullable(Int32),
    `ENTITY_ID` Nullable(String),
    `s.STATUS_ID` Nullable(String),
    `s.NAME` Nullable(String),
    `NAME_INIT` Nullable(String),
    `SORT` Nullable(Int32),
    `SYSTEM` Nullable(String),
    `CATEGORY_ID` Nullable(Int32),
    `COLOR` Nullable(String),
    `SEMANTICS` Nullable(String),
    `EXTRA` Nullable(String),
    `UTM_CAMPAIGN_ID` Nullable(String),
    `UTM_CAMPAIGN_PURE` String,
    `UTM_SOURCE_PURE` String,
    `UTM_MEDIUM_PURE` String
)
ENGINE = SummingMergeTree
ORDER BY (ID);

-- 2. materialized view (updates data from now)
DROP VIEW IF EXISTS DB.mart_mkt_bx_crm_lead_mv;
CREATE MATERIALIZED VIEW DB.mart_mkt_bx_crm_lead_mv TO DB.mart_mkt_bx_crm_lead AS
SELECT
    *,
    CASE
        WHEN toUInt64OrNull(SUBSTRING(`UTM_CAMPAIGN`, LENGTH(`UTM_CAMPAIGN`) - POSITION(REVERSE(`UTM_CAMPAIGN`), '_')+2, LENGTH(`UTM_CAMPAIGN`))) IS NOT NULL THEN SUBSTRING(`UTM_CAMPAIGN`, LENGTH(`UTM_CAMPAIGN`) - POSITION(REVERSE(`UTM_CAMPAIGN`), '_')+2, LENGTH(`UTM_CAMPAIGN`))
		ELSE IFNULL(`UTM_CAMPAIGN`, '')
    END AS UTM_CAMPAIGN_ID,
    CASE 
        WHEN `UTM_CAMPAIGN`='(referral)' THEN ''
        WHEN `UTM_CAMPAIGN`='(organic)' THEN ''
        WHEN `UTM_CAMPAIGN`='(none)' THEN ''
        WHEN `UTM_CAMPAIGN`='(undefined)' THEN ''
        ELSE IFNULL(`UTM_CAMPAIGN`, '')
    END as UTM_CAMPAIGN_PURE,
    CASE 
        WHEN `UTM_SOURCE`='(offline)' THEN IFNULL(`UTM_MEDIUM`, '')
        WHEN `UTM_SOURCE`='' THEN IFNULL(s.NAME, '')
        WHEN `UTM_SOURCE`='(none)' THEN IFNULL(s.NAME, '')
        WHEN `UTM_SOURCE`='(direct)' THEN IFNULL(s.NAME, '')
        WHEN `UTM_SOURCE` IS NULL THEN IFNULL(s.NAME, '')
        ELSE IFNULL(`UTM_SOURCE`, '')
    END as UTM_SOURCE_PURE,
    CASE
        WHEN `UTM_SOURCE`='(direct)' THEN IFNULL(s.NAME, 'direct')
        WHEN `UTM_MEDIUM`='' THEN IFNULL(s.NAME, 'direct')
        WHEN `UTM_MEDIUM` IS NULL THEN IFNULL(s.NAME, 'direct')
        WHEN `UTM_MEDIUM`='(none)' THEN IFNULL(s.NAME, 'direct')
        ELSE IFNULL(`UTM_MEDIUM`,IFNULL(s.NAME, 'direct'))
    END as UTM_MEDIUM_PURE
FROM DB.raw_bx_crm_lead as l
    LEFT JOIN DB.raw_bx_crm_status as s ON l.SOURCE_ID=s.STATUS_ID

SETTINGS join_use_nulls = 1;

-- 3. initial data upload
INSERT INTO DB.mart_mkt_bx_crm_lead SELECT
    *,
    CASE
        WHEN toUInt64OrNull(SUBSTRING(`UTM_CAMPAIGN`, LENGTH(`UTM_CAMPAIGN`) - POSITION(REVERSE(`UTM_CAMPAIGN`), '_')+2, LENGTH(`UTM_CAMPAIGN`))) IS NOT NULL THEN SUBSTRING(`UTM_CAMPAIGN`, LENGTH(`UTM_CAMPAIGN`) - POSITION(REVERSE(`UTM_CAMPAIGN`), '_')+2, LENGTH(`UTM_CAMPAIGN`))
		ELSE IFNULL(`UTM_CAMPAIGN`, '')
    END AS UTM_CAMPAIGN_ID,
    CASE 
        WHEN `UTM_CAMPAIGN`='(referral)' THEN ''
        WHEN `UTM_CAMPAIGN`='(organic)' THEN ''
        WHEN `UTM_CAMPAIGN`='(none)' THEN ''
        WHEN `UTM_CAMPAIGN`='(undefined)' THEN ''
        ELSE IFNULL(`UTM_CAMPAIGN`, '')
    END as UTM_CAMPAIGN_PURE,
    CASE 
        WHEN `UTM_SOURCE`='(offline)' THEN IFNULL(`UTM_MEDIUM`, '')
        WHEN `UTM_SOURCE`='' THEN IFNULL(s.NAME, '')
        WHEN `UTM_SOURCE`='(none)' THEN IFNULL(s.NAME, '')
        WHEN `UTM_SOURCE`='(direct)' THEN IFNULL(s.NAME, '')
        WHEN `UTM_SOURCE` IS NULL THEN IFNULL(s.NAME, '')
        ELSE IFNULL(`UTM_SOURCE`, '')
    END as UTM_SOURCE_PURE,
    CASE
        WHEN `UTM_SOURCE`='(direct)' THEN IFNULL(s.NAME, 'direct')
        WHEN `UTM_MEDIUM`='' THEN IFNULL(s.NAME, 'direct')
        WHEN `UTM_MEDIUM` IS NULL THEN IFNULL(s.NAME, 'direct')
        WHEN `UTM_MEDIUM`='(none)' THEN IFNULL(s.NAME, 'direct')
        ELSE IFNULL(`UTM_MEDIUM`, IFNULL(s.NAME, 'direct'))
    END as UTM_MEDIUM_PURE
FROM DB.raw_bx_crm_lead as l
    LEFT JOIN DB.raw_bx_crm_status as s ON l.SOURCE_ID=s.STATUS_ID

SETTINGS join_use_nulls = 1;