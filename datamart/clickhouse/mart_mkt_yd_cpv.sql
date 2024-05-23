-- 1. ground table
CREATE OR REPLACE TABLE DB.mart_mkt_yd_cpv
(
    `DT` Date,
	`CPV` Float64,
	`C` Float64,
	`V` Int64,
	`UTM_CAMPAIGN_PURE` String
)
ENGINE = SummingMergeTree
ORDER BY (DT, CPV, C, V, UTM_CAMPAIGN_PURE);

-- 2. materialized view (updates data rom now)
DROP VIEW IF EXISTS DB.mart_mkt_yd_cpv_mv;
CREATE MATERIALIZED VIEW DB.mart_mkt_yd_cpv_mv TO DB.mart_mkt_yd_cpv AS
SELECT
	DT,
	MIN(COSTS)/(SUM(VISITS)+0.00000001) AS CPV,
	MIN(COSTS) AS C,
	SUM(VISITS) AS V,
	UTM_CAMPAIGN_PURE
FROM DB.mart_mkt_yd_costs as c
	LEFT JOIN DB.mart_mkt_ym_visits as v ON c.UTM_CAMPAIGN_PURE=v.UTM_CAMPAIGN_PURE AND c.DT=v.DT
GROUP BY DT, UTM_CAMPAIGN_PURE

SETTINGS join_use_nulls = 1;

-- 3. initial data upload
INSERT INTO DB.mart_mkt_yd_cpv SELECT
	DT,
	MIN(COSTS)/(SUM(VISITS)+0.00000001) AS CPV,
	MIN(COSTS) AS C,
	SUM(VISITS) AS V,
	UTM_CAMPAIGN_PURE
FROM DB.mart_mkt_yd_costs as c
	LEFT JOIN DB.mart_mkt_ym_visits as v ON c.UTM_CAMPAIGN_PURE=v.UTM_CAMPAIGN_PURE AND c.DT=v.DT
GROUP BY DT, UTM_CAMPAIGN_PURE

SETTINGS join_use_nulls = 1;