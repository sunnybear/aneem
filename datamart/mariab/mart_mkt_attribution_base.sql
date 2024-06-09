create or replace view mart_ym_goals_purchase as (SELECT
    `ym:s:goalDateTime` as gdt, `ym:s:clientID` as cid
FROM raw_ym_visits_goals
WHERE `ym:s:goalID` in ('40707328', '40707301') AND `ym:s:clientID`>0);

create or replace view mart_ym_clients as (select
	`ym:s:clientID` AS cid,
    `ym:s:dateTime` AS vdt,
    CASE
        WHEN `ym:s:lastUTMMedium`='' THEN `ym:s:lastTrafficSource`
        ELSE IFNULL(`ym:s:lastUTMMedium`, `ym:s:lastTrafficSource`)
    END AS UTMMedium,
    CASE
		WHEN `ym:s:lastUTMMedium`='' OR `ym:s:lastUTMMedium` IS NULL THEN CASE
			WHEN `ym:s:lastTrafficSource`='organic' THEN `ym:s:lastSearchEngine`
			WHEN `ym:s:lastTrafficSource`='referral' THEN `ym:s:lastReferalSource`
			WHEN `ym:s:lastTrafficSource`='ad' THEN `ym:s:lastAdvEngine`
			WHEN `ym:s:lastTrafficSource`='social' THEN `ym:s:lastSocialNetwork`
			WHEN `ym:s:lastTrafficSource`='messenger' THEN `ym:s:lastMessenger`
			ELSE `ym:s:from` END
        ELSE IFNULL(`ym:s:lastUTMSource`, '')
    END AS UTMSource,
    `ym:s:lastUTMCampaign` AS UTMCampaign,
	`ym:s:lastUTMTerm` AS UTMTerm,
	CASE
		WHEN `ym:s:regionCity`='Saint Petersburg' THEN 'SPB'
		WHEN `ym:s:regionCity`='Moscow' THEN 'MSK'
		ELSE 'REGIONS'
	END AS region
FROM raw_ym_visits
WHERE `ym:s:clientID`>0);

create or replace view mart_ym_goals_close as (select
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
	`UTMTerm`,
	region,
	ROW_NUMBER() OVER (PARTITION BY goals.gdt ORDER BY DATEDIFF(goals.gdt, clients.vdt)) AS rowNum
from mart_ym_goals_purchase as goals
    left join mart_ym_clients as clients on goals.cid=clients.cid
WHERE goals.gdt > clients.vdt);

create or replace view mart_ym_goals_utm as (select *
from mart_ym_goals_close
WHERE rowNum=1);

create or replace view mart_bx_orders_datetime as (select
	id,
    YEAR(dateInsert) as odt_year,
	MONTH(dateInsert) as odt_month,
	DAYOFMONTH(dateInsert) as odt_day,
	HOUR(dateInsert) as odt_hour,
    MINUTE(dateInsert) as odt_minute,
	price, dateInsert, canceled, statusId
from raw_bx_orders);

create or replace view mart_bx_orders_datetime_1 as (select
	id,
    YEAR(dateInsert) as odt_year,
	MONTH(dateInsert) as odt_month,
	DAYOFMONTH(dateInsert) as odt_day,
	CASE
		WHEN MINUTE(dateInsert)=0 THEN (HOUR(dateInsert)+23)%24
		ELSE HOUR(dateInsert)
	END as odt_hour,
    (MINUTE(dateInsert)+59)%60 as odt_minute,
	price, dateInsert, canceled, statusId
from raw_bx_orders);

create or replace view mart_bx_orders_utm as (select
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
from mart_ym_goals_utm
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
from mart_ym_goals_utm
left join mart_bx_orders_datetime_1 on odt_year=gdt_year and odt_month=gdt_month and odt_day=gdt_day and odt_hour=gdt_hour and odt_minute=gdt_minute
where odt_minute IS NOT NULL);

create or replace view mart_bx_orders_all as (SELECT
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
FROM mart_bx_orders_utm

UNION ALL

SELECT
	id,
	price,
	dateInsert,
	canceled,
	statusId,
	CASE
		WHEN LOCATE('YAMARKET_', xmlId)>0 THEN 'Яндекс.Маркет'
		WHEN LOCATE('Заказ поступил с Озона', userDescription)>0 THEN 'Озон'
		ELSE 'direct'
	END AS UTMMedium,
	CASE 
		WHEN LOCATE('YAMARKET_', xmlId)>0 THEN 'Yandex.Market'
		WHEN LOCATE('Заказ поступил с Озона', userDescription)>0 THEN 'OZON'
		ELSE ''
	END AS UTMSource,
	'' AS UTMCampaign,
	'' AS UTMTerm,
	'MSK' AS region
FROM raw_bx_orders
WHERE id NOT IN (SELECT id FROM mart_bx_orders_utm));

CREATE OR REPLACE EVENT mart_visits_dt
  ON SCHEDULE EVERY 1 DAY STARTS '2024-01-01 04:00:00.000' DO
CREATE OR REPLACE TABLE `mart_visits_dt` (
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
        WHEN `ym:s:lastUTMMedium`='' THEN `ym:s:lastTrafficSource`
        ELSE IFNULL(`ym:s:lastUTMMedium`, `ym:s:lastTrafficSource`)
    END AS UTMMedium,
    CASE
        WHEN `ym:s:lastTrafficSource`='organic' THEN CASE
            WHEN `ym:s:lastUTMMedium`='' THEN  `ym:s:lastSearchEngine`
            ELSE IFNULL(`ym:s:lastUTMSource`, `ym:s:lastSearchEngine`)
        END
        ELSE `ym:s:lastUTMSource`
    END AS UTMSource,
    `ym:s:lastUTMCampaign` AS UTMCampaign,
	`ym:s:lastUTMTerm` AS UTMTerm,
	CASE
		WHEN `ym:s:regionCity`='Saint Petersburg' THEN 'SPB'
		WHEN `ym:s:regionCity`='Moscow' THEN 'MSK'
		ELSE 'REGIONS'
	END as Region
FROM raw_ym_visits
GROUP BY DATE(`ym:s:dateTime`), UTMMedium, UTMSource, UTMCampaign, UTMTerm, Region;

CREATE OR REPLACE TABLE `mart_visits_dt` (
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
        WHEN `ym:s:lastUTMMedium`='' THEN `ym:s:lastTrafficSource`
        ELSE IFNULL(`ym:s:lastUTMMedium`, `ym:s:lastTrafficSource`)
    END AS UTMMedium,
    CASE
        WHEN `ym:s:lastTrafficSource`='organic' THEN CASE
            WHEN `ym:s:lastUTMMedium`='' THEN  `ym:s:lastSearchEngine`
            ELSE IFNULL(`ym:s:lastUTMSource`, `ym:s:lastSearchEngine`)
        END
        ELSE `ym:s:lastUTMSource`
    END AS UTMSource,
    `ym:s:lastUTMCampaign` AS UTMCampaign,
	`ym:s:lastUTMTerm` AS UTMTerm,
	CASE
		WHEN `ym:s:regionCity`='Saint Petersburg' THEN 'SPB'
		WHEN `ym:s:regionCity`='Moscow' THEN 'MSK'
		ELSE 'REGIONS'
	END as Region
FROM raw_ym_visits
GROUP BY DATE(`ym:s:dateTime`), UTMMedium, UTMSource, UTMCampaign, UTMTerm, Region;

CREATE OR REPLACE EVENT mart_costs_dt
  ON SCHEDULE EVERY 1 DAY STARTS '2024-01-01 04:20:00.000' DO
CREATE OR REPLACE TABLE `mart_costs_dt` (
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
    DATE(`Date`) AS `DT`,
	0 AS 'Visits',
	SUM(`Cost`) as Costs,
	0 AS Orders,
	0 AS Sales,
	0 AS Revenue,
    IFNULL(u.UTMMedium, 'cpc') AS UTMMedium,
	IFNULL(u.UTMSource, 'yandex') AS UTMSource,
    c.CampaignName as UTMCampaign,
	'' AS UTMTerm,
	'MSK' AS Region
FROM raw_yd_costs as c
    LEFT JOIN raw_yd_campaigns_utms as u ON c.CampaignId=u.CampaignId
GROUP BY DATE(`Date`), UTMMedium, UTMSource, UTMCampaign, UTMTerm, Region;

CREATE OR REPLACE TABLE `mart_costs_dt` (
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
    DATE(`Date`) AS `DT`,
	0 AS 'Visits',
	SUM(`Cost`) as Costs,
	0 AS Orders,
	0 AS Sales,
	0 AS Revenue,
    IFNULL(u.UTMMedium, 'cpc') AS UTMMedium,
	IFNULL(u.UTMSource, 'yandex') AS UTMSource,
    c.CampaignName as UTMCampaign,
	'' AS UTMTerm,
	'MSK' AS Region
FROM raw_yd_costs as c
    LEFT JOIN raw_yd_campaigns_utms as u ON c.CampaignId=u.CampaignId
GROUP BY DATE(`Date`), UTMMedium, UTMSource, UTMCampaign, UTMTerm, Region;

create or replace view mart_orders_dt as (SELECT
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
FROM mart_bx_orders_all
GROUP BY DATE(dateInsert), UTMMedium, UTMSource, UTMCampaign, UTMTerm, Region);

create or replace view mart_sales_dt as (SELECT
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
FROM mart_bx_orders_all
WHERE statusId IN ('D', 'F', 'G', 'OG', 'P', 'YA')
GROUP BY DATE(dateInsert), UTMMedium, UTMSource, UTMCampaign, UTMTerm, Region);

create or replace view mart_sales_1c_dt as (SELECT
	DATE(`Дата_Реализации`) AS `DT`,
	0 AS Visits,
	0 AS Costs,
	0 AS Orders,
	COUNT(distinct `Номер_1с`) AS Sales,
	SUM(`Сумма`) as Revenue,
	'store' as UTMMedium,
	`Организация` as UTMSource,
	`Контрагент` as UTMCampaign,
	'' as UTMTerm,
	CASE
		WHEN `Организация`='Магазин МСК' THEN 'MSK'
		WHEN `Организация`='Магазин СПБ' THEN 'SPB'
		ELSE 'REGIONS'
	END AS Region
FROM raw_1c_sales
	WHERE `Контрагент` not in ('Покупатель Маркета', 'Покупатель Ozon', 'Интернет покупатель', 'Покупатель Авито', 'Яндекс.Маркет')
GROUP BY DATE(`Дата_Реализации`), UTMMedium, UTMSource, UTMCampaign, UTMTerm, Region);

create or replace view mart_sales_dt_all as (
SELECT * FROM mart_sales_dt
UNION ALL
SELECT * FROM mart_sales_1c_dt
);

create or replace view mart_mkt_e2e as (SELECT
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
FROM mart_visits_dt
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
FROM mart_orders_dt
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
FROM mart_sales_dt_all
WHERE Revenue>0);

CREATE OR REPLACE EVENT mart_mkt_attribution_base
  ON SCHEDULE EVERY 1 DAY STARTS '2024-01-01 08:00:00.000' DO
   CREATE OR REPLACE TABLE `mart_mkt_attribution_base` (
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
FROM mart_mkt_e2e as e
    LEFT JOIN raw_yd_campaigns_utms as cuid ON CAST(cuid.CampaignId AS CHAR)=e.UTMCampaign
    LEFT JOIN raw_yd_campaigns_utms as cucamp ON cucamp.UTMCampaign=e.UTMCampaign
GROUP BY DT, e.UTMMedium, e.UTMSource, e.UTMCampaign, e.Region;

CREATE OR REPLACE TABLE `mart_mkt_attribution_base` (
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
FROM mart_mkt_e2e as e
    LEFT JOIN raw_yd_campaigns_utms as cuid ON CAST(cuid.CampaignId AS CHAR)=e.UTMCampaign
    LEFT JOIN raw_yd_campaigns_utms as cucamp ON cucamp.UTMCampaign=e.UTMCampaign
GROUP BY DT, e.UTMMedium, e.UTMSource, e.UTMCampaign, e.Region;