-- 1. ground table
CREATE OR REPLACE TABLE DB.mart_mkt_bx_crm_deal
(
	`ID` Int64,
    `TITLE` String,
    `TYPE_ID` String,
    `STAGE_ID` String,
    `PROBABILITY` Float32,
    `CURRENCY_ID` String,
    `OPPORTUNITY` Float32,
    `IS_MANUAL_OPPORTUNITY` String,
    `TAX_VALUE` Float32,
    `LEAD_ID` Int64,
    `COMPANY_ID` Int64,
    `CONTACT_ID` Int64,
    `QUOTE_ID` Int64,
    `BEGINDATE` DateTime,
    `CLOSEDATE` DateTime,
    `ASSIGNED_BY_ID` Int64,
    `CREATED_BY_ID` Int64,
    `MODIFY_BY_ID` Int64,
    `DATE_CREATE` DateTime,
    `DATE_MODIFY` DateTime,
    `OPENED` String,
    `CLOSED` String,
    `COMMENTS` String,
    `ADDITIONAL_INFO` String,
    `LOCATION_ID` Int64,
    `CATEGORY_ID` Int64,
    `STAGE_SEMANTIC_ID` String,
    `IS_NEW` String,
    `IS_RECURRING` String,
    `IS_RETURN_CUSTOMER` String,
    `IS_REPEATED_APPROACH` String,
    `SOURCE_ID` String,
    `SOURCE_DESCRIPTION` String,
    `ORIGINATOR_ID` String,
    `ORIGIN_ID` String,
    `MOVED_BY_ID` Int64,
    `MOVED_TIME` DateTime,
    `LAST_ACTIVITY_TIME` DateTime,
    `UTM_SOURCE` String,
    `UTM_MEDIUM` String,
    `UTM_CAMPAIGN` String,
    `UTM_CONTENT` String,
    `UTM_TERM` String,
    `LAST_ACTIVITY_BY` Int64,
    `ts` Int64,
    `s.ID` Nullable(Int32),
    `ENTITY_ID` Nullable(String),
    `STATUS_ID` Nullable(String),
    `NAME` Nullable(String),
    `NAME_INIT` Nullable(String),
    `SORT` Nullable(Int32),
    `SYSTEM` Nullable(String),
    `s.CATEGORY_ID` Nullable(Int32),
    `COLOR` Nullable(String),
    `SEMANTICS` Nullable(String),
    `EXTRA` Nullable(String),
    `UTM_CAMPAIGN_ID` String,
    `UTM_CAMPAIGN_PURE` String,
    `UTM_SOURCE_PURE` String,
    `UTM_MEDIUM_PURE` String
)
ENGINE = SummingMergeTree
ORDER BY (ID);

-- 2. materialized view (updates data from now)
DROP VIEW IF EXISTS DB.mart_mkt_bx_crm_deal_mv;
CREATE MATERIALIZED VIEW DB.mart_mkt_bx_crm_deal_mv TO DB.mart_mkt_bx_crm_deal AS
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
FROM DB.raw_bx_crm_deal as d
    LEFT JOIN DB.raw_bx_crm_status as s ON d.SOURCE_ID=s.STATUS_ID

SETTINGS join_use_nulls = 1;

-- 3. initial data upload
INSERT INTO DB.mart_mkt_bx_crm_deal SELECT
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
FROM DB.raw_bx_crm_deal as d
    LEFT JOIN DB.raw_bx_crm_status as s ON d.SOURCE_ID=s.STATUS_ID

SETTINGS join_use_nulls = 1;