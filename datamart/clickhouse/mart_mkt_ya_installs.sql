CREATE VIEW DB.mart_mkt_ya_installs AS SELECT
    COUNT(`installation_id`) AS INSTALLS,
    toDate(`install_datetime`) AS DT,
    'app' AS UTM_SOURCE_PURE,
    `tracker_name` AS UTM_MEDIUM_PURE,
    `publisher_name` AS UTM_CAMPAIGN_PURE
FROM DB.raw_ya_installs
GROUP BY UTM_CAMPAIGN_PURE,UTM_MEDIUM_PURE,UTM_SOURCE_PURE,DT

SETTINGS join_use_nulls = 1