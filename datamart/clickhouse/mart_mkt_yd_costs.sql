CREATE VIEW DB.mart_mkt_yd_costs AS SELECT
    SUM(IFNULL(`Cost`,0)) AS COSTS,
    toDate(`Date`) AS DT,
    CASE
        WHEN `AdNetworkType`='AD_NETWORK' THEN 'yandex_network'
        WHEN `AdNetworkType`='SEARCH' THEN 'yandex'
    END AS UTM_SOURCE_PURE,
    toString(`CampaignId`) AS UTM_CAMPAIGN_ID,
    'cpc' AS UTM_MEDIUM_PURE,
    `CampaignName` AS CAMPAIGN_NAME
FROM DB.raw_yd_costs
GROUP BY UTM_CAMPAIGN_ID,CAMPAIGN_NAME,UTM_MEDIUM_PURE,UTM_SOURCE_PURE,DT

SETTINGS join_use_nulls = 1