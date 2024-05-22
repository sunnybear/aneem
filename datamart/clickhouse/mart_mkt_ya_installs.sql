CREATE OR REPLACE VIEW DB.mart_mkt_ya_installs AS SELECT
    COUNT(i.installation_id) AS INSTALLS,
    toDate(i.install_datetime) AS DT,
    CASE 
        WHEN ca.UTM_MEDIUM = '' THEN 'app'
		WHEN ca.UTM_MEDIUM IS NULL THEN 'app'
        ELSE ca.UTM_MEDIUM
    END AS UTM_MEDIUM_PURE,
    CASE 
        WHEN ca.UTM_MEDIUM = '' THEN i.publisher_name
		WHEN ca.UTM_MEDIUM IS NULL THEN i.publisher_name
        ELSE ca.UTM_SOURCE
    END AS UTM_SOURCE_PURE,
    CASE 
        WHEN ca.UTM_MEDIUM = '' THEN i.tracker_name
		WHEN ca.UTM_MEDIUM IS NULL THEN i.tracker_name
        ELSE ca.UTM_CAMPAIGN
    END AS UTM_CAMPAIGN_PURE,
	IFNULL(ca.UTM_TERM, '') AS UTM_TERM_PURE
FROM DB.raw_ya_installs as i
    LEFT JOIN DB.dict_yainstallationid_yclid as ic ON ic.installation_id=i.installation_id
    LEFT JOIN DB.dict_yclid_attribution_lndc as ca ON ca.yclid=ic.yclid
GROUP BY UTM_TERM_PURE, UTM_CAMPAIGN_PURE, UTM_MEDIUM_PURE, UTM_SOURCE_PURE, DT

SETTINGS join_use_nulls = 1