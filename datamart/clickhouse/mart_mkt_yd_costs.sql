CREATE VIEW DB.mart_mkt_yd_costs AS SELECT
    SUM(IFNULL(`c.Cost`,0)) AS COSTS,
    toDate(`c.Date`) AS DT,
    CASE
        WHEN `c.AdNetworkType`='AD_NETWORK' THEN IFNULL(u.UTMSource, 'yandex_network')
        WHEN `c.AdNetworkType`='SEARCH' THEN IFNULL(u.UTMSource, 'yandex')
    END AS UTM_SOURCE_PURE,
    toString(`c.CampaignId`) AS UTM_CAMPAIGN_ID,
    IFNULL(u.UTMMedium, 'cpc') AS UTM_MEDIUM_PURE,
	IFNULL(u.UTMCampaign, c.CampaignId) AS UTM_CAMPAIGN_PURE,
    `c.CampaignName` AS CAMPAIGN_NAME
FROM DB.raw_yd_costs as c
	LEFT JOIN DB.raw_yd_campaigns_utms as u WHERE c.CampaignId=u.CampaignId
GROUP BY UTM_CAMPAIGN_ID,CAMPAIGN_NAME,UTM_CAMPAIGN_PURE,UTM_MEDIUM_PURE,UTM_SOURCE_PURE,DT

SETTINGS join_use_nulls = 1