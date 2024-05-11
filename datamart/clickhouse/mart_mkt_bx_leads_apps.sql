CREATE VIEW DB.mart_mkt_bx_leads_app AS
(WITH leads AS (SELECT
    l.ID as ID,
    LOCATE(TITLE, 'Заказ из приложения') AS LEAD_APP,
	DATE_CREATE,
    CASE 
        WHEN l.UTM_MEDIUM_PURE = '' OR l.UTM_MEDIUM_PURE IS NULL THEN CASE WHEN (ca.UTM_MEDIUM='' OR ca.UTM_MEDIUM IS NULL) AND LEAD_APP THEN 'app' ELSE ca.UTM_MEDIUM END
        ELSE l.UTM_MEDIUM_PURE
    END AS UTM_MEDIUM,
    CASE 
        WHEN l.UTM_MEDIUM_PURE = '' OR l.UTM_MEDIUM_PURE IS NULL THEN CASE WHEN (ca.UTM_MEDIUM='' OR ca.UTM_MEDIUM IS NULL) AND LEAD_APP THEN am.publisher_name ELSE ca.UTM_MEDIUM END
        ELSE l.UTM_SOURCE_PURE
    END AS UTM_SOURCE,
    CASE 
        WHEN l.UTM_MEDIUM_PURE = '' OR l.UTM_MEDIUM_PURE IS NULL THEN CASE WHEN (ca.UTM_MEDIUM='' OR ca.UTM_MEDIUM IS NULL) AND LEAD_APP THEN am.tracker_name ELSE ca.UTM_CAMPAIGN END
        ELSE l.UTM_CAMPAIGN_PURE
    END AS UTM_CAMPAIGN,
	UTM_CAMPAIGN_ID
FROM DB.mart_mkt_bx_crm_lead as l
    LEFT JOIN DB.dict_bxleadid_phone as lp ON l.ID=lp.ID
    LEFT JOIN DB.dict_yainstallationid_phone as ip ON ip.phone=lp.phone
    LEFT JOIN DB.dict_yainstallationid_yclid as ic ON ic.installation_id=ip.installation_id
    LEFT JOIN DB.raw_ya_installs as am ON am.installation_id=ip.installation_id
    LEFT JOIN DB.dict_yclid_attribution_lndc as ca ON ca.yclid=ic.yclid
GROUP BY ID, LEAD_APP, DATE_CREATE, UTM_MEDIUM, UTM_SOURCE, UTM_CAMPAIGN, UTM_CAMPAIGN_ID

SETTINGS join_use_nulls = 1)

SELECT
    count(ID) as LEADS,
    toDate(DATE_CREATE) AS DT,
    UTM_CAMPAIGN_ID AS UTM_CAMPAIGN_ID,
    UTM_CAMPAIGN AS UTM_CAMPAIGN_PURE,
    UTM_SOURCE AS UTM_SOURCE_PURE,
    UTM_MEDIUM AS UTM_MEDIUM_PURE
FROM leads
GROUP BY DT,UTM_CAMPAIGN_ID,UTM_CAMPAIGN,UTM_SOURCE,UTM_MEDIUM)

SETTINGS join_use_nulls = 1