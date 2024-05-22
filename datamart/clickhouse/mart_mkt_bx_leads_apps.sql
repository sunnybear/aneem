-- 1. ground table
CREATE OR REPLACE TABLE DB.mart_mkt_bx_leads_app
(
    `LEADS` Int64,
	`DT` Date,
    `UTM_CAMPAIGN_ID` String,
	`UTM_CAMPAIGN_PURE` String,
	`UTM_SOURCE_PURE` String,
	`UTM_MEDIUM_PURE` String,
	`UTM_TERM_PURE` String
)
ENGINE = SummingMergeTree
ORDER BY (LEADS, DT, UTM_CAMPAIGN_ID, UTM_CAMPAIGN_PURE, UTM_SOURCE_PURE, UTM_MEDIUM_PURE, UTM_TERM_PURE);

-- 2. materialized view (updates data rom now)
DROP VIEW IF EXISTS DB.mart_mkt_bx_leads_app_mv;
CREATE MATERIALIZED VIEW DB.mart_mkt_bx_leads_app_mv TO DB.mart_mkt_bx_leads_app AS
(WITH leads AS (SELECT
    l.ID as ID,
    LOCATE(TITLE, 'Заказ из приложения') AS LEAD_APP,
	DATE_CREATE,
    CASE 
		WHEN l.UTM_MEDIUM_PURE = 'Веб-сайт' AND LEAD_APP THEN CASE WHEN (ca.UTM_MEDIUM='' OR ca.UTM_MEDIUM IS NULL) THEN 'app' ELSE ca.UTM_MEDIUM END
		WHEN l.UTM_MEDIUM_PURE = 'Звонок' THEN CASE WHEN (ctlndc.UTM_MEDIUM='' OR ctlndc.UTM_MEDIUM IS NULL) THEN IFNULL(ca.UTM_MEDIUM, 'Звонок') ELSE ctlndc.UTM_MEDIUM END
        WHEN l.UTM_MEDIUM_PURE = '' OR l.UTM_MEDIUM_PURE IS NULL THEN CASE
			WHEN ca.UTM_MEDIUM<>'' AND ca.UTM_MEDIUM IS NOT NULL THEN ca.UTM_MEDIUM
			WHEN cact.UTM_MEDIUM<>'' AND cact.UTM_MEDIUM IS NOT NULL THEN cact.UTM_MEDIUM
			WHEN LEAD_APP>0 THEN 'app'
			ELSE l.UTM_MEDIUM_PURE END
        ELSE l.UTM_MEDIUM_PURE
    END AS UTM_MEDIUM,
    CASE
		WHEN l.UTM_MEDIUM_PURE = 'Веб-сайт' AND LEAD_APP THEN CASE WHEN (ca.UTM_MEDIUM='' OR ca.UTM_MEDIUM IS NULL) THEN am.publisher_name ELSE ca.UTM_SOURCE END
		WHEN l.UTM_MEDIUM_PURE = 'Звонок' THEN CASE WHEN (ctlndc.UTM_MEDIUM='' OR ctlndc.UTM_MEDIUM IS NULL) THEN IFNULL(ca.UTM_SOURCE, l.UTM_SOURCE_PURE) ELSE ctlndc.UTM_SOURCE END
        WHEN l.UTM_MEDIUM_PURE = '' OR l.UTM_MEDIUM_PURE IS NULL THEN CASE
			WHEN ca.UTM_MEDIUM<>'' AND ca.UTM_MEDIUM IS NOT NULL THEN ca.UTM_SOURCE
			WHEN cact.UTM_MEDIUM<>'' AND cact.UTM_MEDIUM IS NOT NULL THEN cact.UTM_SOURCE
			WHEN LEAD_APP>0 THEN am.publisher_name
			ELSE l.UTM_SOURCE_PURE END
        ELSE l.UTM_SOURCE_PURE
    END AS UTM_SOURCE,
    CASE
		WHEN l.UTM_MEDIUM_PURE = 'Веб-сайт' AND LEAD_APP THEN CASE WHEN (ca.UTM_MEDIUM='' OR ca.UTM_MEDIUM IS NULL) THEN am.tracker_name ELSE ca.UTM_CAMPAIGN END
		WHEN l.UTM_MEDIUM_PURE = 'Звонок' THEN CASE WHEN (ctlndc.UTM_MEDIUM='' OR ctlndc.UTM_MEDIUM IS NULL) THEN IFNULL(ca.UTM_CAMPAIGN, l.UTM_CAMPAIGN_PURE) ELSE ctlndc.UTM_CAMPAIGN END
        WHEN l.UTM_MEDIUM_PURE = '' OR l.UTM_MEDIUM_PURE IS NULL THEN CASE
			WHEN ca.UTM_MEDIUM<>'' AND ca.UTM_MEDIUM IS NOT NULL THEN ca.UTM_CAMPAIGN
			WHEN cact.UTM_MEDIUM<>'' AND cact.UTM_MEDIUM IS NOT NULL THEN cact.UTM_CAMPAIGN
			WHEN LEAD_APP>0 THEN am.tracker_name
			ELSE l.UTM_CAMPAIGN_PURE END
        ELSE l.UTM_CAMPAIGN_PURE
    END AS UTM_CAMPAIGN,
	UTM_CAMPAIGN_ID,
	l.UTM_TERM as UTM_TERM
FROM DB.mart_mkt_bx_crm_lead as l
    LEFT JOIN DB.dict_bxleadid_phone as lp ON l.ID=lp.ID
    LEFT JOIN DB.dict_yainstallationid_phone_all as ip ON ip.phone=lp.phone
    LEFT JOIN DB.dict_yainstallationid_yclid as ic ON ic.installation_id=ip.installation_id
    LEFT JOIN DB.raw_ya_installs as am ON am.installation_id=ip.installation_id
    LEFT JOIN DB.dict_yclid_attribution_lndc as ca ON ca.yclid=ic.yclid
	LEFT JOIN DB.dict_ctphone_yclid as ct ON ct.phone=lp.phone
	LEFT JOIN DB.dict_yclid_attribution_lndc as cact ON cact.yclid=ct.yclid
	LEFT JOIN DB.dict_ctphone_attribution_lndc as ctlndc ON ctlndc.phone=lp.phone
GROUP BY ID, LEAD_APP, DATE_CREATE, UTM_MEDIUM, UTM_SOURCE, UTM_CAMPAIGN, UTM_CAMPAIGN_ID, UTM_TERM

SETTINGS join_use_nulls = 1)

SELECT
    count(ID) as LEADS,
    toDate(DATE_CREATE) AS DT,
    UTM_CAMPAIGN_ID AS UTM_CAMPAIGN_ID,
    IFNULL(cuid.CampaignName, IFNULL(cucamp.CampaignName, UTM_CAMPAIGN)) AS UTM_CAMPAIGN_PURE,
    UTM_SOURCE AS UTM_SOURCE_PURE,
    UTM_MEDIUM AS UTM_MEDIUM_PURE,
	UTM_TERM AS UTM_TERM_PURE
FROM leads as l
	LEFT JOIN DB.raw_yd_campaigns_utms as cuid ON toString(cuid.CampaignId)=l.UTM_CAMPAIGN
    LEFT JOIN DB.raw_yd_campaigns_utms as cucamp ON cucamp.UTMCampaign=l.UTM_CAMPAIGN
GROUP BY DT, UTM_CAMPAIGN_ID, UTM_CAMPAIGN, UTM_SOURCE, UTM_MEDIUM, UTM_TERM, cuid.CampaignName, cucamp.CampaignName

SETTINGS join_use_nulls = 1);

-- 3. initial data upload
INSERT INTO DB.mart_mkt_bx_leads_app SELECT
    count(ID) as LEADS,
    toDate(DATE_CREATE) AS DT,
    UTM_CAMPAIGN_ID AS UTM_CAMPAIGN_ID,
    IFNULL(cuid.CampaignName, IFNULL(cucamp.CampaignName, UTM_CAMPAIGN)) AS UTM_CAMPAIGN_PURE,
    UTM_SOURCE AS UTM_SOURCE_PURE,
    UTM_MEDIUM AS UTM_MEDIUM_PURE,
	UTM_TERM AS UTM_TERM_PURE
FROM (SELECT
    l.ID as ID,
    LOCATE(TITLE, 'Заказ из приложения') AS LEAD_APP,
	DATE_CREATE,
    CASE 
		WHEN l.UTM_MEDIUM_PURE = 'Веб-сайт' AND LEAD_APP THEN CASE WHEN (ca.UTM_MEDIUM='' OR ca.UTM_MEDIUM IS NULL) THEN 'app' ELSE ca.UTM_MEDIUM END
		WHEN l.UTM_MEDIUM_PURE = 'Звонок' THEN CASE WHEN (ctlndc.UTM_MEDIUM='' OR ctlndc.UTM_MEDIUM IS NULL) THEN IFNULL(ca.UTM_MEDIUM, 'Звонок') ELSE ctlndc.UTM_MEDIUM END
        WHEN l.UTM_MEDIUM_PURE = '' OR l.UTM_MEDIUM_PURE IS NULL THEN CASE
			WHEN ca.UTM_MEDIUM<>'' AND ca.UTM_MEDIUM IS NOT NULL THEN ca.UTM_MEDIUM
			WHEN cact.UTM_MEDIUM<>'' AND cact.UTM_MEDIUM IS NOT NULL THEN cact.UTM_MEDIUM
			WHEN LEAD_APP>0 THEN 'app'
			ELSE l.UTM_MEDIUM_PURE END
        ELSE l.UTM_MEDIUM_PURE
    END AS UTM_MEDIUM,
    CASE
		WHEN l.UTM_MEDIUM_PURE = 'Веб-сайт' AND LEAD_APP THEN CASE WHEN (ca.UTM_MEDIUM='' OR ca.UTM_MEDIUM IS NULL) THEN am.publisher_name ELSE ca.UTM_SOURCE END
		WHEN l.UTM_MEDIUM_PURE = 'Звонок' THEN CASE WHEN (ctlndc.UTM_MEDIUM='' OR ctlndc.UTM_MEDIUM IS NULL) THEN IFNULL(ca.UTM_SOURCE, l.UTM_SOURCE_PURE) ELSE ctlndc.UTM_SOURCE END
        WHEN l.UTM_MEDIUM_PURE = '' OR l.UTM_MEDIUM_PURE IS NULL THEN CASE
			WHEN ca.UTM_MEDIUM<>'' AND ca.UTM_MEDIUM IS NOT NULL THEN ca.UTM_SOURCE
			WHEN cact.UTM_MEDIUM<>'' AND cact.UTM_MEDIUM IS NOT NULL THEN cact.UTM_SOURCE
			WHEN LEAD_APP>0 THEN am.publisher_name
			ELSE l.UTM_SOURCE_PURE END
        ELSE l.UTM_SOURCE_PURE
    END AS UTM_SOURCE,
    CASE
		WHEN l.UTM_MEDIUM_PURE = 'Веб-сайт' AND LEAD_APP THEN CASE WHEN (ca.UTM_MEDIUM='' OR ca.UTM_MEDIUM IS NULL) THEN am.tracker_name ELSE ca.UTM_CAMPAIGN END
		WHEN l.UTM_MEDIUM_PURE = 'Звонок' THEN CASE WHEN (ctlndc.UTM_MEDIUM='' OR ctlndc.UTM_MEDIUM IS NULL) THEN IFNULL(ca.UTM_CAMPAIGN, l.UTM_CAMPAIGN_PURE) ELSE ctlndc.UTM_CAMPAIGN END
        WHEN l.UTM_MEDIUM_PURE = '' OR l.UTM_MEDIUM_PURE IS NULL THEN CASE
			WHEN ca.UTM_MEDIUM<>'' AND ca.UTM_MEDIUM IS NOT NULL THEN ca.UTM_CAMPAIGN
			WHEN cact.UTM_MEDIUM<>'' AND cact.UTM_MEDIUM IS NOT NULL THEN cact.UTM_CAMPAIGN
			WHEN LEAD_APP>0 THEN am.tracker_name
			ELSE l.UTM_CAMPAIGN_PURE END
        ELSE l.UTM_CAMPAIGN_PURE
    END AS UTM_CAMPAIGN,
	UTM_CAMPAIGN_ID,
	l.UTM_TERM as UTM_TERM
FROM DB.mart_mkt_bx_crm_lead as l
    LEFT JOIN DB.dict_bxleadid_phone as lp ON l.ID=lp.ID
    LEFT JOIN DB.dict_yainstallationid_phone_all as ip ON ip.phone=lp.phone
    LEFT JOIN DB.dict_yainstallationid_yclid as ic ON ic.installation_id=ip.installation_id
    LEFT JOIN DB.raw_ya_installs as am ON am.installation_id=ip.installation_id
    LEFT JOIN DB.dict_yclid_attribution_lndc as ca ON ca.yclid=ic.yclid
	LEFT JOIN DB.dict_ctphone_yclid as ct ON ct.phone=lp.phone
	LEFT JOIN DB.dict_yclid_attribution_lndc as cact ON cact.yclid=ct.yclid
	LEFT JOIN DB.dict_ctphone_attribution_lndc as ctlndc ON ctlndc.phone=lp.phone
GROUP BY ID, LEAD_APP, DATE_CREATE, UTM_MEDIUM, UTM_SOURCE, UTM_CAMPAIGN, UTM_CAMPAIGN_ID, UTM_TERM) as l
	LEFT JOIN DB.raw_yd_campaigns_utms as cuid ON toString(cuid.CampaignId)=l.UTM_CAMPAIGN
    LEFT JOIN DB.raw_yd_campaigns_utms as cucamp ON cucamp.UTMCampaign=l.UTM_CAMPAIGN
GROUP BY DT, UTM_CAMPAIGN_ID, UTM_CAMPAIGN, UTM_SOURCE, UTM_MEDIUM, UTM_TERM, cuid.CampaignName, cucamp.CampaignName

SETTINGS join_use_nulls = 1;