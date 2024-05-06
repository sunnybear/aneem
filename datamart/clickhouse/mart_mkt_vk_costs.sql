CREATE VIEW DB.mart_mkt_vk_costs AS SELECT
    SUM(IFNULL(`spent`,0)) AS COSTS,
    toDate(`date`) AS DT,
    'vk_ads' UTM_SOURCE_PURE,
    toString(`campaign_id`) AS UTM_CAMPAIGN_ID,
    'social' AS UTM_MEDIUM_PURE
FROM DB.raw_vk_costs
GROUP BY UTM_CAMPAIGN_ID,UTM_MEDIUM_PURE,UTM_SOURCE_PURE,DT

SETTINGS join_use_nulls = 1