-- 1. ground table
CREATE TABLE DB.mart_mkt_bx_deals_app
(
    `DEALS` Int64,
	`REPEATDEALS` Int64,
	`DT` Date,
	`REVENUE` Float64,
    `UTM_CAMPAIGN_ID` String,
	`UTM_CAMPAIGN_PURE` String,
	`UTM_SOURCE_PURE` String,
	`UTM_MEDIUM_PURE` String
)
ENGINE = SummingMergeTree
ORDER BY (DEALS, REPEATDEALS, DT, REVENUE, UTM_CAMPAIGN_ID, UTM_CAMPAIGN_PURE, UTM_SOURCE_PURE, UTM_MEDIUM_PURE);

-- 2. materialized view (updates data rom now)
CREATE MATERIALIZED VIEW DB.mart_mkt_bx_deals_app_mv TO DB.mart_mkt_bx_deals_app AS
(WITH deals AS (SELECT
    d.ID as ID,
    LOCATE(TITLE, 'Заказ из приложения') AS DEAL_APP,
    IS_RETURN_CUSTOMER,
    CLOSEDATE,
    OPPORTUNITY,
    CASE 
		WHEN d.UTM_MEDIUM_PURE = 'Веб-сайт' AND DEAL_APP THEN CASE WHEN (ca.UTM_MEDIUM='' OR ca.UTM_MEDIUM IS NULL) THEN 'app' ELSE ca.UTM_MEDIUM END
		WHEN d.UTM_MEDIUM_PURE = 'Звонок' THEN CASE WHEN (ctlndc.UTM_MEDIUM='' OR ctlndc.UTM_MEDIUM IS NULL) THEN IFNULL(ca.UTM_MEDIUM, 'Звонок') ELSE ctlndc.UTM_MEDIUM END
        WHEN d.UTM_MEDIUM_PURE = '' OR d.UTM_MEDIUM_PURE IS NULL THEN CASE
			WHEN ca.UTM_MEDIUM<>'' AND ca.UTM_MEDIUM IS NOT NULL THEN ca.UTM_MEDIUM
			WHEN cact.UTM_MEDIUM<>'' AND cact.UTM_MEDIUM IS NOT NULL THEN cact.UTM_MEDIUM
			WHEN DEAL_APP>0 THEN 'app'
			ELSE d.UTM_MEDIUM_PURE END
        ELSE d.UTM_MEDIUM_PURE
    END AS UTM_MEDIUM,
    CASE
		WHEN d.UTM_MEDIUM_PURE = 'Веб-сайт' AND DEAL_APP THEN CASE WHEN (ca.UTM_MEDIUM='' OR ca.UTM_MEDIUM IS NULL) THEN am.publisher_name ELSE ca.UTM_SOURCE END
		WHEN d.UTM_MEDIUM_PURE = 'Звонок' THEN CASE WHEN (ctlndc.UTM_MEDIUM='' OR ctlndc.UTM_MEDIUM IS NULL) THEN IFNULL(ca.UTM_SOURCE, d.UTM_SOURCE_PURE) ELSE ctlndc.UTM_SOURCE END
        WHEN d.UTM_MEDIUM_PURE = '' OR d.UTM_MEDIUM_PURE IS NULL THEN CASE
			WHEN ca.UTM_MEDIUM<>'' AND ca.UTM_MEDIUM IS NOT NULL THEN ca.UTM_SOURCE
			WHEN cact.UTM_MEDIUM<>'' AND cact.UTM_MEDIUM IS NOT NULL THEN cact.UTM_SOURCE
			WHEN DEAL_APP>0 THEN am.publisher_name
			ELSE d.UTM_SOURCE_PURE END
        ELSE d.UTM_SOURCE_PURE
    END AS UTM_SOURCE,
    CASE
		WHEN d.UTM_MEDIUM_PURE = 'Веб-сайт' AND DEAL_APP THEN CASE WHEN (ca.UTM_MEDIUM='' OR ca.UTM_MEDIUM IS NULL) THEN am.tracker_name ELSE ca.UTM_CAMPAIGN END
		WHEN d.UTM_MEDIUM_PURE = 'Звонок' THEN CASE WHEN (ctlndc.UTM_MEDIUM='' OR ctlndc.UTM_MEDIUM IS NULL) THEN IFNULL(ca.UTM_CAMPAIGN, d.UTM_CAMPAIGN_PURE) ELSE ctlndc.UTM_CAMPAIGN END
        WHEN d.UTM_MEDIUM_PURE = '' OR d.UTM_MEDIUM_PURE IS NULL THEN CASE
			WHEN ca.UTM_MEDIUM<>'' AND ca.UTM_MEDIUM IS NOT NULL THEN ca.UTM_CAMPAIGN
			WHEN cact.UTM_MEDIUM<>'' AND cact.UTM_MEDIUM IS NOT NULL THEN cact.UTM_CAMPAIGN
			WHEN DEAL_APP>0 THEN am.tracker_name
			ELSE d.UTM_CAMPAIGN_PURE END
        ELSE d.UTM_CAMPAIGN_PURE
    END AS UTM_CAMPAIGN,
	UTM_CAMPAIGN_ID
FROM DB.mart_mkt_bx_crm_deal as d
    LEFT JOIN DB.dict_bxdealid_phone as dp ON d.ID=dp.ID
    LEFT JOIN DB.dict_yainstallationid_phone as ip ON ip.phone=dp.phone
    LEFT JOIN DB.dict_yainstallationid_yclid as ic ON ic.installation_id=ip.installation_id
    LEFT JOIN DB.raw_ya_installs as am ON am.installation_id=ip.installation_id
    LEFT JOIN DB.dict_yclid_attribution_lndc as ca ON ca.yclid=ic.yclid
	LEFT JOIN DB.dict_ctphone_yclid as ct ON ct.phone=dp.phone
	LEFT JOIN DB.dict_yclid_attribution_lndc as cact ON cact.yclid=ct.yclid
	LEFT JOIN DB.dict_ctphone_attribution_lndc as ctlndc ON ctlndc.phone=dp.phone
WHERE
    (CASE
WHEN POSITION(REVERSE(`STAGE_ID`), ':')>0 THEN SUBSTRING(`STAGE_ID`, LENGTH(`STAGE_ID`)-POSITION(REVERSE(`STAGE_ID`), ':')+2, LENGTH(`STAGE_ID`))
ELSE `STAGE_ID`
END) = 'WON'
GROUP BY d.ID, DEAL_APP, IS_RETURN_CUSTOMER, CLOSEDATE, OPPORTUNITY, UTM_MEDIUM, UTM_SOURCE, UTM_CAMPAIGN, UTM_CAMPAIGN_ID

SETTINGS join_use_nulls = 1)

SELECT
    count(ID) as DEALS,
    countIf(IS_RETURN_CUSTOMER='Y') as REPEATDEALS,
    toDate(CLOSEDATE) AS DT,
    sum(OPPORTUNITY) as REVENUE,
    UTM_CAMPAIGN_ID AS UTM_CAMPAIGN_ID,
    IFNULL(cuid.CampaignName, IFNULL(cucamp.CampaignName, UTM_CAMPAIGN)) AS UTM_CAMPAIGN_PURE,
    UTM_SOURCE AS UTM_SOURCE_PURE,
    UTM_MEDIUM AS UTM_MEDIUM_PURE
FROM deals as d
	LEFT JOIN DB.raw_yd_campaigns_utms as cuid ON toString(cuid.CampaignId)=d.UTM_CAMPAIGN
    LEFT JOIN DB.raw_yd_campaigns_utms as cucamp ON cucamp.UTMCampaign=d.UTM_CAMPAIGN
GROUP BY DT,UTM_CAMPAIGN_ID,UTM_CAMPAIGN,UTM_SOURCE,UTM_MEDIUM,cuid.CampaignName,cucamp.CampaignName

SETTINGS join_use_nulls = 1);

-- 3. initial data upload
INSERT INTO DB.mart_mkt_bx_deals_app SELECT
    count(ID) as DEALS,
    countIf(IS_RETURN_CUSTOMER='Y') as REPEATDEALS,
    toDate(CLOSEDATE) AS DT,
    sum(OPPORTUNITY) as REVENUE,
    UTM_CAMPAIGN_ID AS UTM_CAMPAIGN_ID,
    IFNULL(cuid.CampaignName, IFNULL(cucamp.CampaignName, UTM_CAMPAIGN)) AS UTM_CAMPAIGN_PURE,
    UTM_SOURCE AS UTM_SOURCE_PURE,
    UTM_MEDIUM AS UTM_MEDIUM_PURE
FROM (SELECT
    d.ID as ID,
    LOCATE(TITLE, 'Заказ из приложения') AS DEAL_APP,
    IS_RETURN_CUSTOMER,
    CLOSEDATE,
    OPPORTUNITY,
    CASE 
		WHEN d.UTM_MEDIUM_PURE = 'Веб-сайт' AND DEAL_APP THEN CASE WHEN (ca.UTM_MEDIUM='' OR ca.UTM_MEDIUM IS NULL) THEN 'app' ELSE ca.UTM_MEDIUM END
		WHEN d.UTM_MEDIUM_PURE = 'Звонок' THEN CASE WHEN (ctlndc.UTM_MEDIUM='' OR ctlndc.UTM_MEDIUM IS NULL) THEN IFNULL(ca.UTM_MEDIUM, 'Звонок') ELSE ctlndc.UTM_MEDIUM END
        WHEN d.UTM_MEDIUM_PURE = '' OR d.UTM_MEDIUM_PURE IS NULL THEN CASE
			WHEN ca.UTM_MEDIUM<>'' AND ca.UTM_MEDIUM IS NOT NULL THEN ca.UTM_MEDIUM
			WHEN cact.UTM_MEDIUM<>'' AND cact.UTM_MEDIUM IS NOT NULL THEN cact.UTM_MEDIUM
			WHEN DEAL_APP>0 THEN 'app'
			ELSE d.UTM_MEDIUM_PURE END
        ELSE d.UTM_MEDIUM_PURE
    END AS UTM_MEDIUM,
    CASE
		WHEN d.UTM_MEDIUM_PURE = 'Веб-сайт' AND DEAL_APP THEN CASE WHEN (ca.UTM_MEDIUM='' OR ca.UTM_MEDIUM IS NULL) THEN am.publisher_name ELSE ca.UTM_SOURCE END
		WHEN d.UTM_MEDIUM_PURE = 'Звонок' THEN CASE WHEN (ctlndc.UTM_MEDIUM='' OR ctlndc.UTM_MEDIUM IS NULL) THEN IFNULL(ca.UTM_SOURCE, d.UTM_SOURCE_PURE) ELSE ctlndc.UTM_SOURCE END
        WHEN d.UTM_MEDIUM_PURE = '' OR d.UTM_MEDIUM_PURE IS NULL THEN CASE
			WHEN ca.UTM_MEDIUM<>'' AND ca.UTM_MEDIUM IS NOT NULL THEN ca.UTM_SOURCE
			WHEN cact.UTM_MEDIUM<>'' AND cact.UTM_MEDIUM IS NOT NULL THEN cact.UTM_SOURCE
			WHEN DEAL_APP>0 THEN am.publisher_name
			ELSE d.UTM_SOURCE_PURE END
        ELSE d.UTM_SOURCE_PURE
    END AS UTM_SOURCE,
    CASE
		WHEN d.UTM_MEDIUM_PURE = 'Веб-сайт' AND DEAL_APP THEN CASE WHEN (ca.UTM_MEDIUM='' OR ca.UTM_MEDIUM IS NULL) THEN am.tracker_name ELSE ca.UTM_CAMPAIGN END
		WHEN d.UTM_MEDIUM_PURE = 'Звонок' THEN CASE WHEN (ctlndc.UTM_MEDIUM='' OR ctlndc.UTM_MEDIUM IS NULL) THEN IFNULL(ca.UTM_CAMPAIGN, d.UTM_CAMPAIGN_PURE) ELSE ctlndc.UTM_CAMPAIGN END
        WHEN d.UTM_MEDIUM_PURE = '' OR d.UTM_MEDIUM_PURE IS NULL THEN CASE
			WHEN ca.UTM_MEDIUM<>'' AND ca.UTM_MEDIUM IS NOT NULL THEN ca.UTM_CAMPAIGN
			WHEN cact.UTM_MEDIUM<>'' AND cact.UTM_MEDIUM IS NOT NULL THEN cact.UTM_CAMPAIGN
			WHEN DEAL_APP>0 THEN am.tracker_name
			ELSE d.UTM_CAMPAIGN_PURE END
        ELSE d.UTM_CAMPAIGN_PURE
    END AS UTM_CAMPAIGN,
	UTM_CAMPAIGN_ID
FROM DB.mart_mkt_bx_crm_deal as d
    LEFT JOIN DB.dict_bxdealid_phone as dp ON d.ID=dp.ID
    LEFT JOIN DB.dict_yainstallationid_phone as ip ON ip.phone=dp.phone
    LEFT JOIN DB.dict_yainstallationid_yclid as ic ON ic.installation_id=ip.installation_id
    LEFT JOIN DB.raw_ya_installs as am ON am.installation_id=ip.installation_id
    LEFT JOIN DB.dict_yclid_attribution_lndc as ca ON ca.yclid=ic.yclid
	LEFT JOIN DB.dict_ctphone_yclid as ct ON ct.phone=dp.phone
	LEFT JOIN DB.dict_yclid_attribution_lndc as cact ON cact.yclid=ct.yclid
	LEFT JOIN DB.dict_ctphone_attribution_lndc as ctlndc ON ctlndc.phone=dp.phone
WHERE
    (CASE
WHEN POSITION(REVERSE(`STAGE_ID`), ':')>0 THEN SUBSTRING(`STAGE_ID`, LENGTH(`STAGE_ID`)-POSITION(REVERSE(`STAGE_ID`), ':')+2, LENGTH(`STAGE_ID`))
ELSE `STAGE_ID`
END) = 'WON'
GROUP BY d.ID, DEAL_APP, IS_RETURN_CUSTOMER, CLOSEDATE, OPPORTUNITY, UTM_MEDIUM, UTM_SOURCE, UTM_CAMPAIGN, UTM_CAMPAIGN_ID) as d
    LEFT JOIN DB.raw_yd_campaigns_utms as cuid ON toString(cuid.CampaignId)=d.UTM_CAMPAIGN
    LEFT JOIN DB.raw_yd_campaigns_utms as cucamp ON cucamp.UTMCampaign=d.UTM_CAMPAIGN
GROUP BY DT,UTM_CAMPAIGN_ID,UTM_CAMPAIGN,UTM_SOURCE,UTM_MEDIUM,cuid.CampaignName,cucamp.CampaignName

SETTINGS join_use_nulls = 1