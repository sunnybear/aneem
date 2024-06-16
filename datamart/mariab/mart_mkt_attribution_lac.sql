CREATE OR REPLACE EVENT mart_visits_dt_lac
  ON SCHEDULE EVERY 1 DAY STARTS '2024-01-01 04:10:00.000' DO
CREATE OR REPLACE TABLE `mart_visits_dt_lac` (
  `DT` datetime DEFAULT NULL,
  `Visits` bigint(20) DEFAULT NULL,
  `Costs` double DEFAULT NULL,
  `Orders` bigint(20) DEFAULT NULL,
  `Sales` bigint(20) DEFAULT NULL,
  `Revenue` double DEFAULT NULL,
  `UTMMedium` text DEFAULT NULL,
  `UTMSource` text DEFAULT NULL,
  `UTMCampaign` text DEFAULT NULL,
  `UTMTerm` text DEFAULT NULL,
  `Region` text DEFAULT NULL,
  KEY `ix_datetime` (`DT`),
  KEY `ix_channel` (`UTMMedium`(768)),
  KEY `ix_source` (`UTMSource`(768)),
  KEY `ix_campaign` (`UTMCampaign`(768)),
  KEY `ix_term` (`UTMTerm`(768)),
  KEY `ix_region` (`Region`(768))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 SELECT
    DATE(`ym:s:dateTime`) AS `DT`,
    COUNT(`ym:s:visitID`) AS 'Visits',
    0 AS Costs,
    0 AS Orders,
    0 AS Sales,
    0 AS Revenue,
    CASE
        WHEN `ym:s:automaticUTMMedium`='' THEN `ym:s:lastTrafficSource`
        ELSE IFNULL(`ym:s:automaticUTMMedium`, `ym:s:lastTrafficSource`)
    END AS UTMMedium,
    CASE
        WHEN `ym:s:automaticUTMMedium`='' OR `ym:s:automaticUTMMedium` IS NULL THEN CASE
            WHEN `ym:s:lastTrafficSource`='organic' THEN `ym:s:lastSearchEngine`
            WHEN `ym:s:lastTrafficSource`='referral' THEN `ym:s:lastReferalSource`
            WHEN `ym:s:lastTrafficSource`='ad' THEN `ym:s:lastAdvEngine`
            WHEN `ym:s:lastTrafficSource`='social' THEN `ym:s:lastSocialNetwork`
            WHEN `ym:s:lastTrafficSource`='messenger' THEN `ym:s:lastMessenger`
            ELSE `ym:s:from` END
        ELSE IFNULL(`ym:s:automaticUTMSource`, '')
    END AS UTMSource,
    `ym:s:automaticUTMCampaign` AS UTMCampaign,
    `ym:s:automaticUTMTerm` AS UTMTerm,
    CASE
        WHEN `ym:s:regionCity`='Saint Petersburg' THEN 'SPB'
        WHEN `ym:s:regionCity`='Moscow' THEN 'MSK'
        ELSE 'REGIONS'
    END as Region
FROM raw_ym_visits
GROUP BY DATE(`ym:s:dateTime`), UTMMedium, UTMSource, UTMCampaign, UTMTerm, Region;

CREATE OR REPLACE TABLE `mart_visits_dt_lac` (
  `DT` datetime DEFAULT NULL,
  `Visits` bigint(20) DEFAULT NULL,
  `Costs` double DEFAULT NULL,
  `Orders` bigint(20) DEFAULT NULL,
  `Sales` bigint(20) DEFAULT NULL,
  `Revenue` double DEFAULT NULL,
  `UTMMedium` text DEFAULT NULL,
  `UTMSource` text DEFAULT NULL,
  `UTMCampaign` text DEFAULT NULL,
  `UTMTerm` text DEFAULT NULL,
  `Region` text DEFAULT NULL,
  KEY `ix_datetime` (`DT`),
  KEY `ix_channel` (`UTMMedium`(768)),
  KEY `ix_source` (`UTMSource`(768)),
  KEY `ix_campaign` (`UTMCampaign`(768)),
  KEY `ix_term` (`UTMTerm`(768)),
  KEY `ix_region` (`Region`(768))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 SELECT
    DATE(`ym:s:dateTime`) AS `DT`,
    COUNT(`ym:s:visitID`) AS 'Visits',
    0 AS Costs,
    0 AS Orders,
    0 AS Sales,
    0 AS Revenue,
    CASE
        WHEN `ym:s:automaticUTMMedium`='' THEN `ym:s:lastTrafficSource`
        ELSE IFNULL(`ym:s:automaticUTMMedium`, `ym:s:lastTrafficSource`)
    END AS UTMMedium,
    CASE
        WHEN `ym:s:automaticUTMMedium`='' OR `ym:s:automaticUTMMedium` IS NULL THEN CASE
            WHEN `ym:s:lastTrafficSource`='organic' THEN `ym:s:lastSearchEngine`
            WHEN `ym:s:lastTrafficSource`='referral' THEN `ym:s:lastReferalSource`
            WHEN `ym:s:lastTrafficSource`='ad' THEN `ym:s:lastAdvEngine`
            WHEN `ym:s:lastTrafficSource`='social' THEN `ym:s:lastSocialNetwork`
            WHEN `ym:s:lastTrafficSource`='messenger' THEN `ym:s:lastMessenger`
            ELSE `ym:s:from` END
        ELSE IFNULL(`ym:s:automaticUTMSource`, '')
    END AS UTMSource,
    `ym:s:automaticUTMCampaign` AS UTMCampaign,
    `ym:s:automaticUTMTerm` AS UTMTerm,
    CASE
        WHEN `ym:s:regionCity`='Saint Petersburg' THEN 'SPB'
        WHEN `ym:s:regionCity`='Moscow' THEN 'MSK'
        ELSE 'REGIONS'
    END as Region
FROM raw_ym_visits
GROUP BY DATE(`ym:s:dateTime`), UTMMedium, UTMSource, UTMCampaign, UTMTerm, Region;

create or replace view mart_ym_clients_lac as (select
    `ym:s:clientID` AS cid,
    `ym:s:dateTime` AS vdt,
    CASE
        WHEN `ym:s:automaticUTMMedium`='' THEN `ym:s:lastTrafficSource`
        ELSE IFNULL(`ym:s:automaticUTMMedium`, `ym:s:lastTrafficSource`)
    END AS UTMMedium,
    CASE
        WHEN `ym:s:automaticUTMMedium`='' OR `ym:s:automaticUTMMedium` IS NULL THEN CASE
            WHEN `ym:s:lastTrafficSource`='organic' THEN `ym:s:lastSearchEngine`
            WHEN `ym:s:lastTrafficSource`='referral' THEN `ym:s:lastReferalSource`
            WHEN `ym:s:lastTrafficSource`='ad' THEN `ym:s:lastAdvEngine`
            WHEN `ym:s:lastTrafficSource`='social' THEN `ym:s:lastSocialNetwork`
            WHEN `ym:s:lastTrafficSource`='messenger' THEN `ym:s:lastMessenger`
            ELSE `ym:s:from` END
        ELSE IFNULL(`ym:s:automaticUTMSource`, '')
    END AS UTMSource,
    `ym:s:automaticUTMCampaign` AS UTMCampaign,
    `ym:s:automaticUTMTerm` AS UTMTerm,
    CASE
        WHEN `ym:s:regionCity`='Saint Petersburg' THEN 'SPB'
        WHEN `ym:s:regionCity`='Moscow' THEN 'MSK'
        ELSE 'REGIONS'
    END AS region
FROM raw_ym_visits
WHERE `ym:s:clientID`>0);

create or replace view mart_ym_goals_close_lac as (select
    YEAR(goals.gdt) as gdt_year,
    MONTH(goals.gdt) as gdt_month,
    DAYOFMONTH(goals.gdt) as gdt_day,
    HOUR(goals.gdt) as gdt_hour,
    MINUTE(goals.gdt) as gdt_minute,
    clients.vdt,
    clients.cid,
    `UTMMedium`,
    `UTMSource`,
    `UTMCampaign`,
	UTMTerm,
    region,
    ROW_NUMBER() OVER (PARTITION BY goals.gdt ORDER BY DATEDIFF(goals.gdt, clients.vdt)) AS rowNum
from mart_ym_goals_purchase as goals
    left join mart_ym_clients_lac as clients on goals.cid=clients.cid
WHERE
    goals.gdt > clients.vdt AND
    `UTMMedium` NOT IN ('direct', 'internal'));

create or replace view mart_ym_goals_utm_lac as (select *
from mart_ym_goals_close_lac
WHERE rowNum=1);

create or replace view mart_bx_orders_utm_lac as (select
    id,
    price,
    dateInsert,
    canceled,
    statusId,
    UTMMedium,
    UTMSource,
    UTMCampaign,
	UTMTerm,
    region
from mart_ym_goals_utm_lac
left join mart_bx_orders_datetime on odt_year=gdt_year and odt_month=gdt_month and odt_day=gdt_day and odt_hour=gdt_hour and odt_minute=gdt_minute
where odt_minute IS NOT NULL

UNION ALL

select
    id,
    price,
    dateInsert,
    canceled,
    statusId,
    UTMMedium,
    UTMSource,
    UTMCampaign,
	UTMTerm,
    region
from mart_ym_goals_utm_lac
left join mart_bx_orders_datetime_1 on odt_year=gdt_year and odt_month=gdt_month and odt_day=gdt_day and odt_hour=gdt_hour and odt_minute=gdt_minute
where odt_minute IS NOT NULL);

create or replace view mart_bx_orders_all_lac as (SELECT
    id,
    price,
    dateInsert,
    canceled,
    statusId,
    UTMMedium,
    UTMSource,
    UTMCampaign,
	UTMTerm,
    region
FROM mart_bx_orders_utm_lac

UNION ALL

SELECT
    id,
    price,
    dateInsert,
    canceled,
    statusId,
    CASE
        WHEN LOCATE('YAMARKET_', xmlId)>0 THEN 'Яндекс.Маркет'
        WHEN LOCATE('AVITO_', xmlId)>0 THEN 'Авито'
        WHEN LOCATE('Заказ поступил с Озона', userDescription)>0 THEN 'Озон'
        ELSE 'direct'
    END AS UTMMedium,
    CASE 
        WHEN LOCATE('YAMARKET_', xmlId)>0 THEN 'Yandex.Market'
        WHEN LOCATE('AVITO_', xmlId)>0 THEN 'Avito'
        WHEN LOCATE('Заказ поступил с Озона', userDescription)>0 THEN 'OZON'
        ELSE ''
    END AS UTMSource,
    '' AS UTMCampaign,
	'' AS UTMTerm,
    'MSK' AS region
FROM raw_bx_orders
WHERE id NOT IN (SELECT id FROM mart_bx_orders_utm_lac));

create or replace view mart_orders_dt_lac as (SELECT
    DATE(dateInsert) AS `DT`,
    0 as Visits,
    0 as Costs,
    COUNT(id) AS 'Orders',
    0 AS Sales,
    0 AS Revenue,
    UTMMedium,
    UTMSource,
    UTMCampaign,
	UTMTerm,
    region AS Region
FROM mart_bx_orders_all_lac
GROUP BY DATE(dateInsert), UTMMedium, UTMSource, UTMCampaign, Region);

create or replace view mart_sales_dt_lac as (SELECT
    DATE(dateInsert) AS `DT`,
    0 AS Visits,
    0 AS Costs,
    0 AS Orders,
    COUNT(id) AS Sales,
    SUM(price) as Revenue,
    UTMMedium,
    UTMSource,
    UTMCampaign,
	UTMTerm,
    region AS Region
FROM mart_bx_orders_all_lac
WHERE statusId IN ('D', 'F', 'G', 'OG', 'P', 'YA')
GROUP BY DATE(dateInsert), UTMMedium, UTMSource, UTMCampaign, Region);

create or replace view mart_sales_dt_all_lac as (
SELECT * FROM mart_sales_dt_lac
UNION ALL
SELECT * FROM mart_sales_1c_dt
);

create or replace view mart_mkt_e2e_lac as (SELECT
    DT,
    Visits,
    Costs,
    Orders,
    Sales,
    Revenue,
    UTMMedium,
    UTMSource,
    UTMCampaign,
    Region
FROM mart_visits_dt_lac
WHERE Visits>0

UNION ALL

SELECT
    DT,
    Visits,
    Costs,
    Orders,
    Sales,
    Revenue,
    UTMMedium,
    UTMSource,
    UTMCampaign,
    Region
FROM mart_costs_dt
WHERE Costs>0

UNION ALL

SELECT
    DT,
    Visits,
    Costs,
    Orders,
    Sales,
    Revenue,
    UTMMedium,
    UTMSource,
    UTMCampaign,
    Region
FROM mart_orders_dt_lac
WHERE Orders>0

UNION ALL 

SELECT
    DT,
    Visits,
    Costs,
    Orders,
    Sales,
    Revenue,
    UTMMedium,
    UTMSource,
    UTMCampaign,
    Region
FROM mart_sales_dt_all_lac
WHERE Revenue>0);

CREATE OR REPLACE EVENT mart_mkt_attribution_lac
  ON SCHEDULE EVERY 1 DAY STARTS '2024-01-01 08:40:00.000' DO
   CREATE OR REPLACE TABLE `mart_mkt_attribution_lac` (
  `_Дата` datetime DEFAULT NULL,
  `_Визиты` bigint(20) DEFAULT NULL,
  `_Расходы` double DEFAULT NULL,
  `_Заказы` bigint(20) DEFAULT NULL,
  `_Продажи` bigint(20) DEFAULT NULL,
  `_Выручка` double DEFAULT NULL,
  `_Канал` text DEFAULT NULL,
  `_Источник` text DEFAULT NULL,
  `_Кампания` text DEFAULT NULL,
  `_Регион` text DEFAULT NULL,
  KEY `ix_datetime` (`_Дата`),
  KEY `ix_channel` (`_Канал`(768)),
  KEY `ix_source` (`_Источник`(768)),
  KEY `ix_campaign` (`_Кампания`(768)),
  KEY `ix_region` (`_Регион`(768))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 SELECT
    DT as '_Дата',
    SUM(Visits) as '_Визиты',
    SUM(Costs) as '_Расходы',
    SUM(Orders) as '_Заказы',
    SUM(Sales) as '_Продажи',
    SUM(Revenue) as '_Выручка',
    e.UTMMedium as '_Канал',
    e.UTMSource as '_Источник',
    REPLACE(IFNULL(cuid.CampaignName, IFNULL(cucamp.CampaignName, e.UTMCampaign)), " ", " ") as '_Кампания',
    Region as '_Регион'
FROM mart_mkt_e2e_lac as e
    LEFT JOIN raw_yd_campaigns_utms as cuid ON CAST(cuid.CampaignId AS CHAR)=e.UTMCampaign
    LEFT JOIN raw_yd_campaigns_utms as cucamp ON cucamp.UTMCampaign=e.UTMCampaign
GROUP BY DT, e.UTMMedium, e.UTMSource, e.UTMCampaign, e.Region;

CREATE OR REPLACE TABLE `mart_mkt_attribution_lac` (
  `_Дата` datetime DEFAULT NULL,
  `_Визиты` bigint(20) DEFAULT NULL,
  `_Расходы` double DEFAULT NULL,
  `_Заказы` bigint(20) DEFAULT NULL,
  `_Продажи` bigint(20) DEFAULT NULL,
  `_Выручка` double DEFAULT NULL,
  `_Канал` text DEFAULT NULL,
  `_Источник` text DEFAULT NULL,
  `_Кампания` text DEFAULT NULL,
  `_Регион` text DEFAULT NULL,
  KEY `ix_datetime` (`_Дата`),
  KEY `ix_channel` (`_Канал`(768)),
  KEY `ix_source` (`_Источник`(768)),
  KEY `ix_campaign` (`_Кампания`(768)),
  KEY `ix_region` (`_Регион`(768))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 SELECT
    DT as '_Дата',
    SUM(Visits) as '_Визиты',
    SUM(Costs) as '_Расходы',
    SUM(Orders) as '_Заказы',
    SUM(Sales) as '_Продажи',
    SUM(Revenue) as '_Выручка',
    e.UTMMedium as '_Канал',
    e.UTMSource as '_Источник',
    REPLACE(IFNULL(cuid.CampaignName, IFNULL(cucamp.CampaignName, e.UTMCampaign)), " ", " ") as '_Кампания',
    Region as '_Регион'
FROM mart_mkt_e2e_lac as e
    LEFT JOIN raw_yd_campaigns_utms as cuid ON CAST(cuid.CampaignId AS CHAR)=e.UTMCampaign
    LEFT JOIN raw_yd_campaigns_utms as cucamp ON cucamp.UTMCampaign=e.UTMCampaign
GROUP BY DT, e.UTMMedium, e.UTMSource, e.UTMCampaign, e.Region;