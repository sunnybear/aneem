-- CPV = Cost Per Visit
CREATE OR REPLACE VIEW mart_mkt_yd_cpv AS SELECT
    c.DT,
    MIN(c.Costs)/(SUM(v.Visits)+0.00000001) AS CPV,
    MIN(c.Costs) AS C,
    SUM(v.Visits) AS V,
    REPLACE(c.UTMCampaign, ' ', ' ') AS UTMCampaign
FROM mart_costs_dt as c
    LEFT JOIN mart_visits_dt as v ON c.UTMCampaign=v.UTMCampaign AND c.DT=v.DT
GROUP BY c.DT, UTMCampaign;

-- orders --
CREATE OR REPLACE VIEW mart_mkt_yd_campaigns_keywords_orders AS SELECT
    DT AS 'Date',
    0 AS 'Impressions',
    0 AS 'Clicks',
    Visits,
    Costs,
    Orders,
    Sales,
    Revenue,
    UTMSource AS 'Source',
    UTMCampaign AS 'Campaign',
    UTMTerm AS 'Term',
    Region
FROM
    mart_orders_dt
WHERE UTMMedium='cpc'
GROUP BY Term, Campaign, Source, Date, Region;

-- sales --
CREATE OR REPLACE VIEW mart_mkt_yd_campaigns_keywords_sales AS
SELECT
    DT AS 'Date',
    0 AS 'Impressions',
    0 AS 'Clicks',
    Visits,
    Costs,
    Orders,
Sales,
Revenue,
    UTMSource AS 'Source',
    UTMCampaign AS 'Campaign',
    UTMTerm AS 'Term',
Region
FROM
    mart_sales_dt
WHERE UTMMedium='cpc'
GROUP BY Term, Campaign, Source, Date, Region;

-- visits/costs --
CREATE OR REPLACE VIEW mart_mkt_yd_campaigns_keywords_visits AS
SELECT
    v.DT AS 'Date',
    0 AS 'Impressions',
    0 AS 'Clicks',
    SUM(v.VISITS) AS 'Visits',
    SUM(v.VISITS*c.CPV) AS 'Costs',
    0 AS 'Orders',
    0 AS 'Sales',
    0.0 AS 'Revenue',
    v.UTMSource AS 'Source',
    v.UTMCampaign AS 'Campaign',
    v.UTMTerm AS 'Term',
    Region
FROM mart_visits_dt as v
    LEFT JOIN mart_mkt_yd_cpv as c ON c.UTMCampaign=v.UTMCampaign AND c.DT=v.DT
WHERE v.UTMMedium='cpc'
GROUP BY Term, Campaign, Source, Date;

-- costs w/o visits --
CREATE OR REPLACE VIEW mart_mkt_yd_campaigns_keywords_costs AS
SELECT
    c.DT AS 'Date',
    0 AS 'Impressions',
    0 AS 'Clicks',
    0 AS 'Visits',
    SUM(c.C) AS 'Costs',
    0 AS 'Orders',
    0 AS 'Sales',
    0.0 AS 'Revenue',
    'yandex' AS 'Source',
    c.UTMCampaign AS 'Campaign',
    '' AS 'Term',
    'MSK' AS Region
FROM mart_mkt_yd_cpv as c
WHERE c.V=0 OR c.V IS NULL
GROUP BY Term, Campaign, Source, Date;

-- clicks/impressions visits --
CREATE OR REPLACE VIEW mart_mkt_yd_campaigns_keywords_clicks AS
SELECT
    DATE(`Date`) AS 'Date',
    SUM(Impressions) AS 'Impressions',
    SUM(Clicks) AS 'Clicks',
    0 AS 'Visits',
    0 AS 'Costs',
    0 AS 'Orders',
    0 AS 'Sales',
    0.0 AS 'Revenue',
    'yandex' AS 'Source',
    REPLACE(IFNULL(u.CampaignName, c.CampaignName), ' ', ' ') AS 'Campaign',
    '' AS 'Term',
    'MSK' AS Region
FROM raw_yd_costs as c
    LEFT JOIN raw_yd_campaigns_utms as u ON c.CampaignId=u.CampaignId
GROUP BY Term, Campaign, Source, Date;

-- alltogether --
CREATE OR REPLACE EVENT mart_mkt_yd_campaigns_keywords
  ON SCHEDULE EVERY 1 DAY STARTS '2024-01-01 08:30:00.000' DO
  CREATE OR REPLACE TABLE `mart_mkt_yd_campaigns_keywords` (
  `_Дата` datetime DEFAULT NULL,
  `_Показы` bigint(20) DEFAULT NULL,
  `_Клики` bigint(20) DEFAULT NULL,
  `_Визиты` bigint(20) DEFAULT NULL,
  `_Расходы` double DEFAULT NULL,
  `_Заказы` bigint(20) DEFAULT NULL,
  `_Продажи` bigint(20) DEFAULT NULL,
  `_Выручка` double DEFAULT NULL,
  `_Канал` text DEFAULT NULL,
  `_Источник` text DEFAULT NULL,
  `_Кампания` text DEFAULT NULL,
  `_Ключевое слово` text DEFAULT NULL,
  `_Регион` text DEFAULT NULL,
  KEY `ix_datetime` (`_Дата`),
  KEY `ix_channel` (`_Канал`(768)),
  KEY `ix_source` (`_Источник`(768)),
  KEY `ix_campaign` (`_Кампания`(768)),
  KEY `ix_term` (`_Ключевое слово`(768)),
  KEY `ix_region` (`_Регион`(768))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 SELECT
`Date` as '_Дата',
    IFNULL(SUM(`Impressions`), 0) AS '_Показы',
    IFNULL(SUM(`Clicks`), 0) AS '_Клики',
    IFNULL(SUM(`Visits`), 0) AS '_Визиты',
    IFNULL(SUM(`Costs`), 0.0) AS '_Расходы',
    IFNULL(SUM(`Orders`), 0) AS '_Заказы',
    IFNULL(SUM(`Sales`), 0) AS '_Продажи',
    IFNULL(SUM(`Revenue`), 0.0) AS '_Выручка',
    CASE
        WHEN `Source`='<не указано>' THEN ''
        WHEN `Source`='<не заполнено>' THEN ''
        ELSE IFNULL(`Source`, '')
    END  AS '_Источник',
    CASE
        WHEN `Campaign`='rsa' THEN 'Общая РСЯ'
        WHEN `Campaign`='<не указано>' THEN ''
        WHEN `Campaign`='<не заполнено>' THEN ''
        ELSE REPLACE(IFNULL(cuid.CampaignName, IFNULL(cucamp.CampaignName, IFNULL(e.Campaign, ''))), ' ', ' ')
    END AS '_Кампания',
    IFNULL(`Term`, '') AS '_Ключевое слово'
FROM
(SELECT * FROM mart_mkt_yd_campaigns_keywords_orders
    UNION ALL
SELECT * FROM mart_mkt_yd_campaigns_keywords_sales
    UNION ALL
SELECT * FROM mart_mkt_yd_campaigns_keywords_visits
    UNION ALL
SELECT * FROM mart_mkt_yd_campaigns_keywords_costs
    UNION ALL
SELECT * FROM mart_mkt_yd_campaigns_keywords_clicks) as e
LEFT JOIN raw_yd_campaigns_utms as cuid ON CAST(cuid.CampaignId AS CHAR)=e.Campaign
    LEFT JOIN raw_yd_campaigns_utms as cucamp ON cucamp.UTMCampaign=e.Campaign
GROUP BY Source, Campaign, Term, Date;

CREATE OR REPLACE TABLE `mart_mkt_yd_campaigns_keywords` (
  `_Дата` datetime DEFAULT NULL,
  `_Показы` bigint(20) DEFAULT NULL,
  `_Клики` bigint(20) DEFAULT NULL,
  `_Визиты` bigint(20) DEFAULT NULL,
  `_Расходы` double DEFAULT NULL,
  `_Заказы` bigint(20) DEFAULT NULL,
  `_Продажи` bigint(20) DEFAULT NULL,
  `_Выручка` double DEFAULT NULL,
  `_Канал` text DEFAULT NULL,
  `_Источник` text DEFAULT NULL,
  `_Кампания` text DEFAULT NULL,
  `_Ключевое слово` text DEFAULT NULL,
  `_Регион` text DEFAULT NULL,
  KEY `ix_datetime` (`_Дата`),
  KEY `ix_channel` (`_Канал`(768)),
  KEY `ix_source` (`_Источник`(768)),
  KEY `ix_campaign` (`_Кампания`(768)),
  KEY `ix_term` (`_Ключевое слово`(768)),
  KEY `ix_region` (`_Регион`(768))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 SELECT
    `Date` as '_Дата',
    IFNULL(SUM(`Impressions`), 0) AS '_Показы',
    IFNULL(SUM(`Clicks`), 0) AS '_Клики',
    IFNULL(SUM(`Visits`), 0) AS '_Визиты',
    IFNULL(SUM(`Costs`), 0.0) AS '_Расходы',
    IFNULL(SUM(`Orders`), 0) AS '_Заказы',
    IFNULL(SUM(`Sales`), 0) AS '_Продажи',
    IFNULL(SUM(`Revenue`), 0.0) AS '_Выручка',
    CASE
        WHEN `Source`='<не указано>' THEN ''
        WHEN `Source`='<не заполнено>' THEN ''
        ELSE IFNULL(`Source`, '')
    END  AS '_Источник',
    CASE
        WHEN `Campaign`='rsa' THEN 'Общая РСЯ'
        WHEN `Campaign`='<не указано>' THEN ''
        WHEN `Campaign`='<не заполнено>' THEN ''
        ELSE REPLACE(IFNULL(cuid.CampaignName, IFNULL(cucamp.CampaignName, IFNULL(e.Campaign, ''))), ' ', ' ')
    END AS '_Кампания',
    IFNULL(`Term`, '') AS '_Ключевое слово'
FROM
(SELECT * FROM mart_mkt_yd_campaigns_keywords_orders
    UNION ALL
SELECT * FROM mart_mkt_yd_campaigns_keywords_sales
    UNION ALL
SELECT * FROM mart_mkt_yd_campaigns_keywords_visits
    UNION ALL
SELECT * FROM mart_mkt_yd_campaigns_keywords_costs
    UNION ALL
SELECT * FROM mart_mkt_yd_campaigns_keywords_clicks) as e
LEFT JOIN raw_yd_campaigns_utms as cuid ON CAST(cuid.CampaignId AS CHAR)=e.Campaign
    LEFT JOIN raw_yd_campaigns_utms as cucamp ON cucamp.UTMCampaign=e.Campaign
GROUP BY Source, Campaign, Term, Date;