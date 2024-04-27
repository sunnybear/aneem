CREATE VIEW DB.mart_mkt_bx_crm_deal AS
SELECT
    *,
    CASE
        WHEN toUInt64OrNull(SUBSTRING(`UTM_CAMPAIGN`, LENGTH(`UTM_CAMPAIGN`) - POSITION(REVERSE(`UTM_CAMPAIGN`), '_')+2, LENGTH(`UTM_CAMPAIGN`))) IS NOT NULL THEN SUBSTRING(`UTM_CAMPAIGN`, LENGTH(`UTM_CAMPAIGN`) - POSITION(REVERSE(`UTM_CAMPAIGN`), '_')+2, LENGTH(`UTM_CAMPAIGN`))
    END AS UTM_CAMPAIGN_ID,
    CASE 
        WHEN `UTM_CAMPAIGN`='(referral)' THEN ''
        WHEN `UTM_CAMPAIGN`='(organic)' THEN ''
        WHEN `UTM_CAMPAIGN`='(none)' THEN ''
        WHEN `UTM_CAMPAIGN`='(undefined)' THEN ''
        ELSE `UTM_CAMPAIGN`
    END as UTM_CAMPAIGN_PURE,
    CASE 
        WHEN `UTM_SOURCE`='(offline)' THEN `UTM_MEDIUM`
        WHEN `UTM_SOURCE`='' THEN IFNULL(`s.NAME`, '')
        WHEN `UTM_SOURCE`='(none)' THEN IFNULL(`s.NAME`, '')
        WHEN `UTM_SOURCE`='(direct)' THEN IFNULL(`s.NAME`, '')
        WHEN `UTM_SOURCE` IS NULL THEN IFNULL(`s.NAME`, '')
        ELSE IFNULL(`UTM_SOURCE`, '')
    END as UTM_SOURCE_PURE,
    CASE
        WHEN `UTM_SOURCE`='(direct)' THEN IFNULL(`s.NAME`, 'direct')
        WHEN `UTM_MEDIUM`='' THEN IFNULL(`s.NAME`, 'direct')
        WHEN `UTM_MEDIUM` IS NULL THEN IFNULL(`s.NAME`, 'direct')
        WHEN `UTM_MEDIUM`='(none)' THEN IFNULL(`s.NAME`, 'direct')
        ELSE IFNULL(`UTM_MEDIUM`,IFNULL(`s.NAME`, 'direct'))
    END as UTM_MEDIUM_PURE
FROM DB.raw_bx_crm_deal as d
    LEFT JOIN DB.raw_bx_crm_status as s ON l.SOURCE_ID=s.STATUS_ID

SETTINGS join_use_nulls = 1