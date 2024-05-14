create view mart_ym_goals_purchase as (SELECT
    `ym:s:goalDateTime` as gdt, `ym:s:clientID` as cid
FROM raw_ym_visits_goals
WHERE `ym:s:goalID` in ('40707328', '40707301') AND `ym:s:clientID`>0);

create view mart_ym_clients as (select
	`ym:s:clientID` as cid,
    `ym:s:dateTime` as vdt,
    CASE
        WHEN `ym:s:lastUTMMedium`='' THEN `ym:s:lastTrafficSource`
        ELSE IFNULL(`ym:s:lastUTMMedium`, `ym:s:lastTrafficSource`)
    END as UTMMedium,
    CASE
        WHEN `ym:s:lastTrafficSource`='organic' THEN CASE
            WHEN `ym:s:lastUTMMedium`='' THEN  `ym:s:lastSearchEngine`
            ELSE IFNULL(`ym:s:lastUTMSource`, `ym:s:lastSearchEngine`)
        END
        ELSE `ym:s:lastUTMSource`
    END as UTMSource,
    `ym:s:lastUTMCampaign` as UTMCampaign
FROM raw_ym_visits
WHERE `ym:s:clientID`>0);

create view mart_ym_goals_close as (select
	YEAR(goals.gdt) as gdt_year,
    MONTH(goals.gdt) as gdt_month,
    DAYOFMONTH(goals.gdt) as gdt_day,
    HOUR(goals.gdt) as gdt_hour,
    MINUTE(goals.gdt) as gdt_minute,
	clients.vdt, clients.cid, `UTMMedium`,`UTMSource`, `UTMCampaign`,
	ROW_NUMBER() OVER (PARTITION BY goals.gdt ORDER BY DATEDIFF(goals.gdt, clients.vdt)) AS rowNum
from mart_ym_goals_purchase as goals
    left join mart_ym_clients as clients on goals.cid=clients.cid
WHERE goals.gdt > clients.vdt);

create view mart_ym_goals_utm as (select *
from mart_ym_goals_close
WHERE rowNum=1);

create view mart_bx_orders_datetime as (select
	id,
    YEAR(dateInsert) as odt_year,
	MONTH(dateInsert) as odt_month,
	DAYOFMONTH(dateInsert) as odt_day,
	HOUR(dateInsert) as odt_hour,
    MINUTE(dateInsert) as odt_minute,
	price, dateInsert, canceled, statusId
from raw_bx_orders);

create view mart_bx_orders_datetime_1 as (select
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

create view mart_bx_orders_utm as (select
	id,
	price,
	dateInsert,
	canceled,
	statusId,
	UTMMedium,
	UTMSource,
	UTMCampaign
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
	UTMCampaign
from mart_ym_goals_utm
left join mart_bx_orders_datetime_1 on odt_year=gdt_year and odt_month=gdt_month and odt_day=gdt_day and odt_hour=gdt_hour and odt_minute=gdt_minute
where odt_minute IS NOT NULL);

create view mart_bx_orders_all as (SELECT
	id,
	price,
	dateInsert,
	canceled,
	statusId,
	UTMMedium,
	UTMSource,
	UTMCampaign
FROM mart_bx_orders_utm

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
WHERE id NOT IN (SELECT id FROM mart_bx_orders_utm));

create view mart_visits_dt as (SELECT
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
    `ym:s:lastUTMCampaign` AS UTMCampaign
FROM raw_ym_visits
GROUP BY DATE(`ym:s:dateTime`), UTMMedium, UTMSource, UTMCampaign);

create view mart_costs_dt as (SELECT
    DATE(`Date`) AS `DT`,
	0 'Visits',
	SUM(`Cost`) as Costs,
	0 AS Orders,
	0 AS Sales,
	0 AS Revenue,
    IFNULL(u.UTMMedium, 'cpc') AS UTMMedium,
	IFNULL(u.UTMSource, 'yandex') AS UTMSource,
    IFNULL(u.UTMCampaign, c.CampaignId) AS UTMCampaign
FROM raw_yd_costs as c
LEFT JOIN raw_yd_campaigns_utms as u ON c.CampaignId=u.CampaignId
GROUP BY DATE(`Date`), UTMMedium, UTMSource, UTMCampaign);

create view mart_orders_dt as (SELECT
	DATE(dateInsert) AS `DT`,
	0 as Visits,
	0 as Costs,
	COUNT(id) AS 'Orders',
	0 AS Sales,
	0 AS Revenue,
	UTMMedium,
	UTMSource,
	UTMCampaign
FROM mart_bx_orders_all
GROUP BY DATE(dateInsert), UTMMedium, UTMSource, UTMCampaign);

create view mart_sales_dt as (SELECT
	DATE(dateInsert) AS `DT`,
	0 AS Visits,
	0 AS Costs,
	0 AS Orders,
	COUNT(id) AS Sales,
	SUM(price) as Revenue,
	UTMMedium,
	UTMSource,
	UTMCampaign
FROM mart_bx_orders_all
WHERE statusId IN ('D', 'F', 'G', 'OG', 'P', 'YA')
GROUP BY DATE(dateInsert), UTMMedium, UTMSource, UTMCampaign);

create view mart_mkt_e2e as (SELECT
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
	UTMCampaign
FROM mart_sales_dt
WHERE Revenue>0);

CREATE EVENT mart_mkt_attribution_base
  ON SCHEDULE EVERY 1 DAY STARTS '2024-01-01 04:00:00.000' DO
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
	UTMMedium as '_Канал',
	UTMSource as '_Источник',
	UTMCampaign as '_Кампания'
FROM mart_mkt_e2e
GROUP BY DT, UTMMedium, UTMSource, UTMCampaign;