CREATE VIEW DB.mart_mkt_bx_deals_app AS
(WITH deals AS (SELECT
    d.ID,
	LOCATE(TITLE, 'Заказ из приложения') AS DEAL_APP,
    IS_RETURN_CUSTOMER,
    CLOSEDATE,
    OPPORTUNITY,
    replace(phone1, '+', '') AS phone,
    UTM_MEDIUM,
    UTM_SOURCE,
    UTM_CAMPAIGN
FROM DB.raw_bx_crm_deal as d
    LEFT JOIN DB.raw_bx_crm_contact_uf as c ON d.CONTACT_ID=c.ID
WHERE
    (CASE
WHEN POSITION(REVERSE(`STAGE_ID`), ':')>0 THEN SUBSTRING(`STAGE_ID`, LENGTH(`STAGE_ID`)-POSITION(REVERSE(`STAGE_ID`), ':')+2, LENGTH(`STAGE_ID`))
ELSE `STAGE_ID`
END) = 'WON'
GROUP BY d.ID, DEAL_APP, IS_RETURN_CUSTOMER, CLOSEDATE, OPPORTUNITY, phone, UTM_MEDIUM, UTM_SOURCE, UTM_CAMPAIGN),

apps AS (SELECT
    installation_id,
    replace(replace(simpleJSONExtractRaw(event_json, 'phone'), '\"', ''), '+', '') AS phone
FROM DB.raw_am_events
WHERE phone<>''),

installs AS (SELECT
    installation_id,
    publisher_name,
    tracker_name
FROM DB.raw_am_installs),

deals_apps AS (SELECT
    ID,
    CLOSEDATE,
    IS_RETURN_CUSTOMER,
    OPPORTUNITY,
    CASE
        WHEN UTM_MEDIUM='' THEN CASE WHEN tracker_name<>'' THEN 'app' WHEN DEAL_APP>0 THEN 'app' ELSE 'direct' END
        WHEN UTM_MEDIUM='(direct)' THEN CASE WHEN tracker_name<>'' THEN 'app' WHEN DEAL_APP>0 THEN 'app' ELSE 'direct' END
		WHEN UTM_MEDIUM='(none)' THEN CASE WHEN tracker_name<>'' THEN 'app' WHEN DEAL_APP>0 THEN 'app' ELSE 'direct' END
		WHEN UTM_MEDIUM='<не указано>' THEN CASE WHEN tracker_name<>'' THEN 'app' WHEN DEAL_APP>0 THEN 'app' ELSE 'direct' END
		WHEN UTM_MEDIUM='<не заполнено>' THEN CASE WHEN tracker_name<>'' THEN 'app' WHEN DEAL_APP>0 THEN 'app' ELSE 'direct' END
        WHEN UTM_MEDIUM IS NULL THEN CASE WHEN tracker_name<>'' THEN 'app' WHEN DEAL_APP>0 THEN 'app' ELSE 'direct' END
        ELSE UTM_MEDIUM
    END as UTM_MEDIUM_PURE,
    CASE
        WHEN UTM_MEDIUM='' THEN CASE WHEN tracker_name<>'' THEN tracker_name ELSE UTM_SOURCE END
        WHEN UTM_MEDIUM='(direct)' THEN CASE WHEN tracker_name<>'' THEN tracker_name ELSE UTM_SOURCE END
		WHEN UTM_MEDIUM='(none)' THEN CASE WHEN tracker_name<>'' THEN tracker_name ELSE UTM_SOURCE END
		WHEN UTM_MEDIUM='<не указано>' THEN CASE WHEN tracker_name<>'' THEN tracker_name ELSE UTM_SOURCE END
		WHEN UTM_MEDIUM='<не заполнено>' THEN CASE WHEN tracker_name<>'' THEN tracker_name ELSE UTM_SOURCE END
        WHEN UTM_MEDIUM IS NULL THEN CASE WHEN tracker_name<>'' THEN tracker_name ELSE UTM_SOURCE END
        ELSE UTM_SOURCE
    END as UTM_SOURCE_PURE,
    CASE
        WHEN UTM_MEDIUM='' THEN CASE WHEN tracker_name<>'' THEN publisher_name ELSE UTM_CAMPAIGN END
        WHEN UTM_MEDIUM='(direct)' THEN CASE WHEN tracker_name<>'' THEN publisher_name ELSE UTM_CAMPAIGN END
		WHEN UTM_MEDIUM='(none)' THEN CASE WHEN tracker_name<>'' THEN publisher_name ELSE UTM_CAMPAIGN END
		WHEN UTM_MEDIUM='<не указано>' THEN CASE WHEN tracker_name<>'' THEN publisher_name ELSE UTM_CAMPAIGN END
		WHEN UTM_MEDIUM='<не заполнено>' THEN CASE WHEN tracker_name<>'' THEN publisher_name ELSE UTM_CAMPAIGN END
        WHEN UTM_MEDIUM IS NULL THEN CASE WHEN tracker_name<>'' THEN publisher_name ELSE UTM_CAMPAIGN END
        WHEN UTM_CAMPAIGN='<не указано>' THEN ''
        ELSE UTM_CAMPAIGN
    END as UTM_CAMPAIGN_PURE,
    UTM_CAMPAIGN AS UTM_CAMPAIGN_ID
FROM deals
    LEFT JOIN apps ON (apps.phone=deals.phone)
    LEFT JOIN installs ON (installs.installation_id=apps.installation_id)
GROUP BY ID, CLOSEDATE, IS_RETURN_CUSTOMER, OPPORTUNITY, UTM_MEDIUM_PURE, UTM_SOURCE_PURE, UTM_CAMPAIGN_PURE, UTM_CAMPAIGN_ID)

SELECT
    count(d.ID) as DEALS,
    countIf(d.IS_RETURN_CUSTOMER='Y') as REPEATDEALS,
    toDate(CLOSEDATE) AS DT,
    sum(OPPORTUNITY) as REVENUE,
    UTM_CAMPAIGN_ID,
    UTM_CAMPAIGN_PURE,
    UTM_SOURCE_PURE,
    UTM_MEDIUM_PURE
FROM deals_apps as d
GROUP BY DT,UTM_CAMPAIGN_PURE,UTM_CAMPAIGN_ID,UTM_SOURCE_PURE,UTM_MEDIUM_PURE)

SETTINGS join_use_nulls = 1