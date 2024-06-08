-- leads --

-- 1. ground table
CREATE OR REPLACE TABLE DB.mart_mkt_yd_campaigns_keywords_leads
(
	`Date` Date,
	`Impressions` Int64,
	`Clicks` Int64,
    `Visits` Int64,
	`Costs` Float64,
	`Installs` Int64,
	`Leads` Int64,
	`Deals` Int64,
	`Revenue` Float64,
	`RepeatDeals` Int64,	
	`Source` String,
	`Campaign` String,
	`Term` String
)
ENGINE = SummingMergeTree
ORDER BY (Date, Impressions, Clicks, Visits, Costs, Installs, Leads, Deals, Revenue, RepeatDeals, Source, Campaign, Term);

-- 2. materialized view (updates data rom now)
DROP VIEW IF EXISTS DB.mart_mkt_yd_campaigns_keywords_leads_mv;
CREATE MATERIALIZED VIEW DB.mart_mkt_yd_campaigns_keywords_leads_mv TO DB.mart_mkt_yd_campaigns_keywords_leads AS
SELECT
    DT AS `Date`,
	0 AS `Impressions`,
	0 AS `Clicks`,
    0 AS `Visits`,
    0 AS `Costs`,
    0 AS `Installs`,
    SUM(LEADS) AS `Leads`,
    0 AS `Deals`,
    0 AS `Revenue`,
    0 AS `RepeatDeals`,
	UTM_SOURCE_PURE AS `Source`,
    UTM_CAMPAIGN_PURE AS `Campaign`,
    UTM_TERM_PURE AS `Term`
FROM
    DB.mart_mkt_bx_leads_app
WHERE UTM_MEDIUM_PURE='cpc'
GROUP BY `Term`, `Campaign`, `Source`, `Date`;

-- 3. initial data upload
INSERT INTO DB.mart_mkt_yd_campaigns_keywords_leads SELECT
    DT AS `Date`,
	0 AS `Impressions`,
	0 AS `Clicks`,
    0 AS `Visits`,
    0 AS `Costs`,
    0 AS `Installs`,
    SUM(LEADS) AS `Leads`,
    0 AS `Deals`,
    0 AS `Revenue`,
    0 AS `RepeatDeals`,
	UTM_SOURCE_PURE AS `Source`,
    UTM_CAMPAIGN_PURE AS `Campaign`,
    UTM_TERM_PURE AS `Term`
FROM
    DB.mart_mkt_bx_leads_app
WHERE UTM_MEDIUM_PURE='cpc'
GROUP BY `Term`, `Campaign`, `Source`, `Date`;

-- deals --

-- 1. ground table
CREATE OR REPLACE TABLE DB.mart_mkt_yd_campaigns_keywords_deals
(
	`Date` Date,
	`Impressions` Int64,
	`Clicks` Int64,
    `Visits` Int64,
	`Costs` Float64,
	`Installs` Int64,
	`Leads` Int64,
	`Deals` Int64,
	`Revenue` Float64,
	`RepeatDeals` Int64,	
	`Source` String,
	`Campaign` String,
	`Term` String
)
ENGINE = SummingMergeTree
ORDER BY (Date, Impressions, Clicks, Visits, Costs, Installs, Leads, Deals, Revenue, RepeatDeals, Source, Campaign, Term);

-- 2. materialized view (updates data rom now)
DROP VIEW IF EXISTS DB.mart_mkt_yd_campaigns_keywords_deals_mv;
CREATE MATERIALIZED VIEW DB.mart_mkt_yd_campaigns_keywords_deals_mv TO DB.mart_mkt_yd_campaigns_keywords_deals AS
SELECT
    DT AS `Date`,
	0 AS `Impressions`,
	0 AS `Clicks`,
    0 AS `Visits`,
    0 AS `Costs`,
    0 AS `Leads`,
	0 AS `Installs`,
    SUM(DEALS) AS `Deals`,
    SUM(REVENUE) AS `Revenue`,
    SUM(REPEATDEALS) AS `RepeatDeals`,
    UTM_SOURCE_PURE AS `Source`,
    UTM_CAMPAIGN_PURE AS `Campaign`,
    UTM_TERM_PURE AS `Term`
FROM
    DB.mart_mkt_bx_deals_app
WHERE UTM_MEDIUM_PURE='cpc'
GROUP BY `Term`, `Campaign`, `Source`, `Date`;

-- 3. initial data upload
INSERT INTO DB.mart_mkt_yd_campaigns_keywords_deals SELECT
    DT AS `Date`,
	0 AS `Impressions`,
	0 AS `Clicks`,
    0 AS `Visits`,
    0 AS `Costs`,
    0 AS `Leads`,
	0 AS `Installs`,
    SUM(DEALS) AS `Deals`,
    SUM(REVENUE) AS `Revenue`,
    SUM(REPEATDEALS) AS `RepeatDeals`,
    UTM_SOURCE_PURE AS `Source`,
    UTM_CAMPAIGN_PURE AS `Campaign`,
    UTM_TERM_PURE AS `Term`
FROM
    DB.mart_mkt_bx_deals_app
WHERE UTM_MEDIUM_PURE='cpc'
GROUP BY `Term`, `Campaign`, `Source`, `Date`;

-- visits/costs --

-- 1. ground table
CREATE OR REPLACE TABLE DB.mart_mkt_yd_campaigns_keywords_visits
(
	`Date` Date,
	`Impressions` Int64,
	`Clicks` Int64,
    `Visits` Int64,
	`Costs` Float64,
	`Installs` Int64,
	`Leads` Int64,
	`Deals` Int64,
	`Revenue` Float64,
	`RepeatDeals` Int64,	
	`Source` String,
	`Campaign` String,
	`Term` String
)
ENGINE = SummingMergeTree
ORDER BY (Date, Impressions, Clicks, Visits, Costs, Installs, Leads, Deals, Revenue, RepeatDeals, Source, Campaign, Term);

-- 2. materialized view (updates data rom now)
DROP VIEW IF EXISTS DB.mart_mkt_yd_campaigns_keywords_visits_mv;
CREATE MATERIALIZED VIEW DB.mart_mkt_yd_campaigns_keywords_visits_mv TO DB.mart_mkt_yd_campaigns_keywords_visits AS
SELECT
    v.DT AS `Date`,
	0 AS `Impressions`,
	0 AS `Clicks`,
    SUM(v.VISITS) AS `Visits`,
    SUM(v.VISITS*c.CPV) AS `Costs`,
	0 AS `Installs`,
    0 AS `Leads`,
    0 AS `Deals`,
    0 AS `Revenue`,
    0 AS `RepeatDeals`,
    v.UTM_SOURCE_PURE AS `Source`,
    v.UTM_CAMPAIGN_PURE AS `Campaign`,
	v.UTM_TERM_PURE AS `Term`
FROM DB.mart_mkt_ym_visits as v
	LEFT JOIN DB.mart_mkt_yd_cpv as c ON c.UTM_CAMPAIGN_PURE=v.UTM_CAMPAIGN_PURE AND c.DT=v.DT
WHERE v.UTM_MEDIUM_PURE='cpc'
GROUP BY `Term`, `Campaign`, `Source`, `Date`

SETTINGS join_use_nulls = 1;

-- 3. initial data upload
INSERT INTO DB.mart_mkt_yd_campaigns_keywords_visits SELECT
    v.DT AS `Date`,
	0 AS `Impressions`,
	0 AS `Clicks`,
    SUM(v.VISITS) AS `Visits`,
    SUM(v.VISITS*c.CPV) AS `Costs`,
	0 AS `Installs`,
    0 AS `Leads`,
    0 AS `Deals`,
    0 AS `Revenue`,
    0 AS `RepeatDeals`,
    v.UTM_SOURCE_PURE AS `Source`,
    v.UTM_CAMPAIGN_PURE AS `Campaign`,
	v.UTM_TERM_PURE AS `Term`
FROM DB.mart_mkt_ym_visits as v
	LEFT JOIN DB.mart_mkt_yd_cpv as c ON c.UTM_CAMPAIGN_PURE=v.UTM_CAMPAIGN_PURE AND c.DT=v.DT
GROUP BY `Term`, `Campaign`, `Source`, `Date`

SETTINGS join_use_nulls = 1;

-- costs w/o visits --

-- 1. ground table
CREATE OR REPLACE TABLE DB.mart_mkt_yd_campaigns_keywords_costs
(
	`Date` Date,
	`Impressions` Int64,
	`Clicks` Int64,
    `Visits` Int64,
	`Costs` Float64,
	`Installs` Int64,
	`Leads` Int64,
	`Deals` Int64,
	`Revenue` Float64,
	`RepeatDeals` Int64,	
	`Source` String,
	`Campaign` String,
	`Term` String
)
ENGINE = SummingMergeTree
ORDER BY (Date, Impressions, Clicks, Visits, Costs, Installs, Leads, Deals, Revenue, RepeatDeals, Source, Campaign, Term);

-- 2. materialized view (updates data rom now)
DROP VIEW IF EXISTS DB.mart_mkt_yd_campaigns_keywords_costs_mv;
CREATE MATERIALIZED VIEW DB.mart_mkt_yd_campaigns_keywords_costs_mv TO DB.mart_mkt_yd_campaigns_keywords_costs AS
SELECT
    c.DT AS `Date`,
	0 AS `Impressions`,
	0 AS `Clicks`,
    0 AS `Visits`,
    SUM(c.C) AS `Costs`,
	0 AS `Installs`,
    0 AS `Leads`,
    0 AS `Deals`,
    0 AS `Revenue`,
    0 AS `RepeatDeals`,
    'yandex' AS `Source`,
    c.UTM_CAMPAIGN_PURE AS `Campaign`,
	'' AS `Term`
FROM DB.mart_mkt_yd_cpv as c
WHERE c.V=0
GROUP BY `Term`, `Campaign`, `Source`, `Date`;

-- 3. initial data upload
INSERT INTO DB.mart_mkt_yd_campaigns_keywords_costs SELECT
    c.DT AS `Date`,
	0 AS `Impressions`,
	0 AS `Clicks`,
    0 AS `Visits`,
    SUM(c.C) AS `Costs`,
	0 AS `Installs`,
    0 AS `Leads`,
    0 AS `Deals`,
    0 AS `Revenue`,
    0 AS `RepeatDeals`,
    'yandex' AS `Source`,
    c.UTM_CAMPAIGN_PURE AS `Campaign`,
	'' AS `Term`
FROM DB.mart_mkt_yd_cpv as c
WHERE c.V=0
GROUP BY `Term`, `Campaign`, `Source`, `Date`;

-- clicks/impressions visits --

-- 1. ground table
CREATE OR REPLACE TABLE DB.mart_mkt_yd_campaigns_keywords_clicks
(
	`Date` Date,
	`Impressions` Int64,
	`Clicks` Int64,
    `Visits` Int64,
	`Costs` Float64,
	`Installs` Int64,
	`Leads` Int64,
	`Deals` Int64,
	`Revenue` Float64,
	`RepeatDeals` Int64,	
	`Source` String,
	`Campaign` String,
	`Term` String
)
ENGINE = SummingMergeTree
ORDER BY (Date, Impressions, Clicks, Visits, Costs, Installs, Leads, Deals, Revenue, RepeatDeals, Source, Campaign, Term);

-- 2. materialized view (updates data rom now)
DROP VIEW IF EXISTS DB.mart_mkt_yd_campaigns_keywords_clicks_mv;
CREATE MATERIALIZED VIEW DB.mart_mkt_yd_campaigns_keywords_clicks_mv TO DB.mart_mkt_yd_campaigns_keywords_clicks AS
SELECT
    DT AS `Date`,
	SUM(IMPRESSIONS) AS `Impressions`,
	SUM(CLICKS) AS `Clicks`,
    0 AS `Visits`,
    0 AS `Costs`,
	0 AS `Installs`,
    0 AS `Leads`,
    0 AS `Deals`,
    0 AS `Revenue`,
    0 AS `RepeatDeals`,
    'yandex' AS `Source`,
    UTM_CAMPAIGN_PURE AS `Campaign`,
	'' AS `Term`
FROM DB.mart_mkt_yd_costs
GROUP BY `Term`, `Campaign`, `Source`, `Date`;

-- 3. initial data upload
INSERT INTO DB.mart_mkt_yd_campaigns_keywords_clicks SELECT
    DT AS `Date`,
	SUM(IMPRESSIONS) AS `Impressions`,
	SUM(CLICKS) AS `Clicks`,
    0 AS `Visits`,
    0 AS `Costs`,
	0 AS `Installs`,
    0 AS `Leads`,
    0 AS `Deals`,
    0 AS `Revenue`,
    0 AS `RepeatDeals`,
    'yandex' AS `Source`,
    UTM_CAMPAIGN_PURE AS `Campaign`,
	'' AS `Term`
FROM DB.mart_mkt_yd_costs
GROUP BY `Term`, `Campaign`, `Source`, `Date`;

-- installs --

-- 1. ground table
CREATE OR REPLACE TABLE DB.mart_mkt_yd_campaigns_keywords_installs
(
	`Date` Date,
	`Impressions` Int64,
	`Clicks` Int64,
    `Visits` Int64,
	`Costs` Float64,
	`Installs` Int64,
	`Leads` Int64,
	`Deals` Int64,
	`Revenue` Float64,
	`RepeatDeals` Int64,	
	`Source` String,
	`Campaign` String,
	`Term` String
)
ENGINE = SummingMergeTree
ORDER BY (Date, Impressions, Clicks, Visits, Costs, Installs, Leads, Deals, Revenue, RepeatDeals, Source, Campaign, Term);

-- 2. materialized view (updates data rom now)
DROP VIEW IF EXISTS DB.mart_mkt_yd_campaigns_keywords_installs_mv;
CREATE MATERIALIZED VIEW DB.mart_mkt_yd_campaigns_keywords_installs_mv TO DB.mart_mkt_yd_campaigns_keywords_installs AS
SELECT
    DT AS `Date`,
	0 AS `Impressions`,
	0 AS `Clicks`,
    0 AS `Visits`,
    0 AS `Costs`,
	SUM(INSTALLS) AS `Installs`,
    0 AS `Leads`,
    0 AS `Deals`,
    0 AS `Revenue`,
    0 AS `RepeatDeals`,
    IFNULL(UTM_SOURCE_PURE, '') AS `Source`,
    IFNULL(UTM_CAMPAIGN_PURE, '') AS `Campaign`,
    IFNULL(UTM_TERM_PURE, '') AS `Term`
FROM
    DB.mart_mkt_ya_installs
WHERE UTM_MEDIUM_PURE='cpc'
GROUP BY `Term`, `Campaign`, `Source`, `Date`;

-- 3. initial data upload
INSERT INTO DB.mart_mkt_yd_campaigns_keywords_installs SELECT
    DT AS `Date`,
	0 AS `Impressions`,
	0 AS `Clicks`,
    0 AS `Visits`,
    0 AS `Costs`,
	SUM(INSTALLS) AS `Installs`,
    0 AS `Leads`,
    0 AS `Deals`,
    0 AS `Revenue`,
    0 AS `RepeatDeals`,
    IFNULL(UTM_SOURCE_PURE, '') AS `Source`,
    IFNULL(UTM_CAMPAIGN_PURE, '') AS `Campaign`,
    IFNULL(UTM_TERM_PURE, '') AS `Term`
FROM
    DB.mart_mkt_ya_installs
WHERE UTM_MEDIUM_PURE='cpc'
GROUP BY `Term`, `Campaign`, `Source`, `Date`;

-- alltogether --

CREATE OR REPLACE VIEW DB.mart_mkt_yd_campaigns_keywords AS 
SELECT
	`Date` as `_Дата`,
	IFNULL(SUM(`Impressions`), 0) AS `_Показы`,
	IFNULL(SUM(`Clicks`), 0) AS `_Клики`,
	IFNULL(SUM(`Visits`), 0) AS `_Визиты`,
    IFNULL(SUM(`Costs`), 0.0) AS `_Расходы`,
    IFNULL(SUM(`Leads`), 0) AS `_Лиды`,
    IFNULL(SUM(`Deals`), 0) AS `_Сделки`,
	IFNULL(SUM(`Installs`), 0) AS `_Установки`,
    IFNULL(SUM(`Revenue`), 0.0) AS `_Выручка`,
    IFNULL(SUM(`RepeatDeals`), 0) AS `_ПовторныеСделки`,
	CASE
		WHEN `Source`='<не указано>' THEN ''
		WHEN `Source`='<не заполнено>' THEN ''
		ELSE IFNULL(`Source`, '')
	END  AS `_Источник`,
	CASE
		WHEN `Campaign`='rsa' THEN 'Общая РСЯ'
		WHEN `Campaign`='<не указано>' THEN ''
		WHEN `Campaign`='<не заполнено>' THEN ''
		ELSE IFNULL(`Campaign`, '') 
	END AS `_Кампания`,
	IFNULL(`Term`, '') AS `_Ключевое слово`
FROM
	(SELECT * FROM DB.mart_mkt_yd_campaigns_keywords_leads
	UNION ALL
	SELECT * FROM DB.mart_mkt_yd_campaigns_keywords_deals
	UNION ALL
	SELECT * FROM DB.mart_mkt_yd_campaigns_keywords_visits
	UNION ALL
	SELECT * FROM DB.mart_mkt_yd_campaigns_keywords_costs
	UNION ALL
	SELECT * FROM DB.mart_mkt_yd_campaigns_keywords_clicks
	UNION ALL
	SELECT * FROM DB.mart_mkt_yd_campaigns_keywords_installs)
GROUP BY `Source`, `Campaign`, `Term`, `Date`