create or replace view mart_ym_goals_close_lndc as (select
	YEAR(goals.gdt) as gdt_year,
    MONTH(goals.gdt) as gdt_month,
    DAYOFMONTH(goals.gdt) as gdt_day,
    HOUR(goals.gdt) as gdt_hour,
    MINUTE(goals.gdt) as gdt_minute,
	clients.vdt, clients.cid, `UTMMedium`,`UTMSource`, `UTMCampaign`,
	ROW_NUMBER() OVER (PARTITION BY goals.gdt ORDER BY DATEDIFF(goals.gdt, clients.vdt)) AS rowNum
from mart_ym_goals_purchase as goals
    left join mart_ym_clients as clients on goals.cid=clients.cid
WHERE
	goals.gdt > clients.vdt AND
	`UTMMedium`NOT IN ('direct', 'internal'));

create or replace view mart_ym_goals_utm_lndc as (select *
from mart_ym_goals_close_lndc
WHERE rowNum=1);

create or replace view mart_bx_orders_utm_lndc as (select
	id,
	price,
	dateInsert,
	canceled,
	statusId,
	UTMMedium,
	UTMSource,
	UTMCampaign
from mart_ym_goals_utm_lndc
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
	UTMCampaign
from mart_ym_goals_utm_lndc
left join mart_bx_orders_datetime_1 on odt_year=gdt_year and odt_month=gdt_month and odt_day=gdt_day and odt_hour=gdt_hour and odt_minute=gdt_minute
where odt_minute IS NOT NULL);

create or replace view mart_bx_orders_all_lndc as (SELECT
	id,
	price,
	dateInsert,
	canceled,
	statusId,
	UTMMedium,
	UTMSource,
	UTMCampaign
FROM mart_bx_orders_utm_lndc

UNION ALL

SELECT
	id,
	price,
	dateInsert,
	canceled,
	statusId,
	CASE
		WHEN LOCATE('YAMARKET_', xmlId)>0 THEN 'marketplace'
		WHEN LOCATE('Заказ поступил с Озона', userDescription)>0 THEN 'marketplace'
		ELSE 'direct'
	END AS UTMMedium,
	CASE 
		WHEN LOCATE('YAMARKET_', xmlId)>0 THEN 'Yandex.Market'
		WHEN LOCATE('Заказ поступил с Озона', userDescription)>0 THEN 'OZON'
		ELSE ''
	END AS UTMSource,
	'' AS UTMCampaign
FROM raw_bx_orders
WHERE id NOT IN (SELECT id FROM mart_bx_orders_utm_lndc));

create or replace view mart_orders_dt_lndc as (SELECT
	DATE(dateInsert) AS `DT`,
	0 as Visits,
	0 as Costs,
	COUNT(id) AS 'Orders',
	0 AS Sales,
	0 AS Revenue,
	UTMMedium,
	UTMSource,
	UTMCampaign
FROM mart_bx_orders_all_lndc
GROUP BY DATE(dateInsert), UTMMedium, UTMSource, UTMCampaign);

create or replace view mart_sales_dt_lndc as (SELECT
	DATE(dateInsert) AS `DT`,
	0 AS Visits,
	0 AS Costs,
	0 AS Orders,
	COUNT(id) AS Sales,
	SUM(price) as Revenue,
	UTMMedium,
	UTMSource,
	UTMCampaign
FROM mart_bx_orders_all_lndc
WHERE statusId IN ('D', 'F', 'G', 'OG', 'P', 'YA')
GROUP BY DATE(dateInsert), UTMMedium, UTMSource, UTMCampaign);

create or replace view mart_mkt_e2e_lndc as (SELECT
	DT,
	Visits,
	Costs,
	Orders,
	Sales,
	Revenue,
	UTMMedium,
	UTMSource,
	UTMCampaign
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
	UTMCampaign
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
	UTMCampaign
FROM mart_orders_dt_lndc
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
	UTMCampaign
FROM mart_sales_dt_lndc
WHERE Revenue>0);

CREATE OR REPLACE EVENT mart_mkt_attribution_lndc
  ON SCHEDULE EVERY 1 DAY STARTS '2024-01-01 08:10:00.000' DO
   CREATE OR REPLACE TABLE `mart_mkt_attribution_lndc` (
  `_Дата` datetime DEFAULT NULL,
  `_Визиты` bigint(20) DEFAULT NULL,
  `_Расходы` double DEFAULT NULL,
  `_Заказы` bigint(20) DEFAULT NULL,
  `_Продажи` bigint(20) DEFAULT NULL,
  `_Выручка` double DEFAULT NULL,
  `_Канал` text DEFAULT NULL,
  `_Источник` text DEFAULT NULL,
  `_Кампания` text DEFAULT NULL,
  KEY `ix_datetime` (`_Дата`),
  KEY `ix_channel` (`_Канал`(768)),
  KEY `ix_source` (`_Источник`(768)),
  KEY `ix_campaign` (`_Кампания`(768))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 SELECT
	DT as '_Дата',
	SUM(Visits) as '_Визиты',
	SUM(Costs) as '_Расходы',
	SUM(Orders) as '_Заказы',
	SUM(Sales) as '_Продажи',
	SUM(Revenue) as '_Выручка',
	e.UTMMedium as '_Канал',
	e.UTMSource as '_Источник',
	IFNULL(cuid.CampaignName, IFNULL(cucamp.CampaignName, e.UTMCampaign)) as '_Кампания'
FROM mart_mkt_e2e_lndc as e
    LEFT JOIN raw_yd_campaigns_utms as cuid ON CAST(cuid.CampaignId AS CHAR)=e.UTMCampaign
    LEFT JOIN raw_yd_campaigns_utms as cucamp ON cucamp.UTMCampaign=e.UTMCampaign
GROUP BY DT, e.UTMMedium, e.UTMSource, e.UTMCampaign;

CREATE OR REPLACE TABLE `mart_mkt_attribution_lndc` (
  `_Дата` datetime DEFAULT NULL,
  `_Визиты` bigint(20) DEFAULT NULL,
  `_Расходы` double DEFAULT NULL,
  `_Заказы` bigint(20) DEFAULT NULL,
  `_Продажи` bigint(20) DEFAULT NULL,
  `_Выручка` double DEFAULT NULL,
  `_Канал` text DEFAULT NULL,
  `_Источник` text DEFAULT NULL,
  `_Кампания` text DEFAULT NULL,
  KEY `ix_datetime` (`_Дата`),
  KEY `ix_channel` (`_Канал`(768)),
  KEY `ix_source` (`_Источник`(768)),
  KEY `ix_campaign` (`_Кампания`(768))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 SELECT
	DT as '_Дата',
	SUM(Visits) as '_Визиты',
	SUM(Costs) as '_Расходы',
	SUM(Orders) as '_Заказы',
	SUM(Sales) as '_Продажи',
	SUM(Revenue) as '_Выручка',
	e.UTMMedium as '_Канал',
	e.UTMSource as '_Источник',
	IFNULL(cuid.CampaignName, IFNULL(cucamp.CampaignName, e.UTMCampaign)) as '_Кампания'
FROM mart_mkt_e2e_lndc as e
    LEFT JOIN raw_yd_campaigns_utms as cuid ON CAST(cuid.CampaignId AS CHAR)=e.UTMCampaign
    LEFT JOIN raw_yd_campaigns_utms as cucamp ON cucamp.UTMCampaign=e.UTMCampaign
GROUP BY DT, e.UTMMedium, e.UTMSource, e.UTMCampaign;