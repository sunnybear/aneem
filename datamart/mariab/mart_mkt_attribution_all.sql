create or replace view mart_mkt_attribution_all as
SELECT
	`_Дата`,
	`_Визиты`,
	`_Расходы`,
	`_Заказы`,
	`_Продажи`,
	`_Выручка`,
	`_Канал`,
	`_Источник`,
	`_Кампания`,
	`_Регион`,
	'LC' as '_Атрибуция'
FROM mart_mkt_attribution_base
UNION ALL
SELECT
	`_Дата`,
	`_Визиты`,
	`_Расходы`,
	`_Заказы`,
	`_Продажи`,
	`_Выручка`,
	`_Канал`,
	`_Источник`,
	`_Кампания`,
	`_Регион`,
	'LPC' as '_Атрибуция'
FROM mart_mkt_attribution_lpc
UNION ALL
SELECT 
	`_Дата`,
	`_Визиты`,
	`_Расходы`,
	`_Заказы`,
	`_Продажи`,
	`_Выручка`,
	`_Канал`,
	`_Источник`,
	`_Кампания`,
	`_Регион`,
	'LNDC' as '_Атрибуция'
FROM mart_mkt_attribution_lndc
UNION ALL
SELECT 
	`_Дата`,
	`_Визиты`,
	`_Расходы`,
	`_Заказы`,
	`_Продажи`,
	`_Выручка`,
	`_Канал`,
	`_Источник`,
	`_Кампания`,
	`_Регион`,
	'LAC' as '_Атрибуция'
FROM mart_mkt_attribution_lac;