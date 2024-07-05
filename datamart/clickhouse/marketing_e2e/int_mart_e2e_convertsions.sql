/* Список конверсий по всей воронке */
/*
	Дата конверсии: conversionDateTime,
	Тип конверсии: conversionType,
	ID конверсии (в источнике): conversionID,
	Источник конверсии: conversionSource,
	Название в источнике конверсии: conversionSourceName,
	Сумма конверсии: conversionSum,
	Yandex Client ID: yclid,
	Google Client ID: gclid,
	Google Analytics ID gaid,
	Yandex AppMetrica Installation ID: amiid,
	Callconversion ID: ctid,
	Телефон: phone,
	Email: email,
	Google (Android) ID: google_aid,
	Android Device ID: oaid,
	iOS Ad ID: ios_ifa,
	iOS Device ID: ios_ifv,
	Windows Devide ID: windows_aid */
/* необходимо выбрать используемые источники */

CREATE OR REPLACE VIEW int_mart_e2e_conversions AS
/* Яндекс.Метрика (цели) */
SELECT
	'goal' AS conversionType,
	'yandex.metrika' AS conversionSource,
	'' AS conversionSourceName,
	`ym:s:goalDateTime` AS conversionDateTime,
	`ym:s:goalID` AS conversionID,
	`ym:s:goalPrice` AS conversionSum,
	`ym:s:clientID` AS yclid,
	'' AS gclid,
	'' AS gaid,
	'' AS amiid,
	'' AS phone,
	'' AS email,
	'' AS google_aid,
	'' AS oaid,
	'' AS ios_ifa,
	'' AS ios_ifv, 
	'' AS windows_aid
FROM
	raw_ym_visits_goals

/* Яндекс.Аппметрика (установки) */
/*
UNION ALL
SELECT
	CASE
		WHEN `is_reinstallation`=1 THEN 'reinstallation'
		ELSE 'installation'
	END AS conversionType,
	'yandex.appmetrica' AS conversionSource,
	`app_package_name` AS conversionSourceName,
	`install_datetime` AS conversionDateTime,
	`installation_id` AS conversionID,
	0.0 AS conversionSum,
	'' AS yclid,
	'' AS gclid,
	'' AS gaid,
	'' AS amiid,
	'' AS phone,
	'' AS email,
	`google_aid` AS google_aid,
	`oaid` AS oaid,
	`ios_ifa` AS ios_ifa,
	`ios_ifv` AS ios_ifv, 
	`windows_aid` AS windows_aid
FROM
	raw_ya_installs
*/
/* Яндекс.Аппметрика (события) */
/*
UNION ALL
SELECT
	CONCAT('ya_', `event_name`) AS conversionType,
	'yandex.appmetrica' AS conversionSource,
	`app_package_name` AS conversionSourceName,
	`event_datetime` AS conversionDateTime,
	toString(`session_id`) AS conversionID,
	0.0 AS conversionSum,
	'' AS yclid,
	'' AS gclid,
	'' AS gaid,
	`installation_id` AS amiid,
	'' AS phone,
	'' AS email,
	`google_aid` AS google_aid,
	'' AS oaid,
	`ios_ifa` AS ios_ifa,
	`ios_ifv` AS ios_ifv,
	`windows_aid` AS windows_aid
FROM
	raw_ya_events
*/
/* Calltouch (звонки) */
/*
UNION ALL
SELECT
	'call' AS conversionType,
	'calltouch' AS conversionSource,
	`hostname` AS conversionSourceName,
	`date` AS conversionDateTime,
	`sipCallId` AS conversionID,
	0.0 AS conversionSum,
	`yaClientId` AS yclid,
	'' as gclid,
	`clientId` AS gaid,
	'' AS amiid,
	toString(`callerNumber`) AS phone,
	'' AS email,
	'' AS google_aid,
	'' AS oaid,
	'' AS ios_ifa,
	'' AS ios_ifv, 
	'' AS windows_aid
FROM
	raw_ct_calls
*/
/* Битрикс24 (лиды) */
/*
UNION ALL
SELECT
	'lead' AS conversionType,
	'bitrix24' AS conversionSource,
	'' AS conversionSourceName,
	`DATE_CREATE` AS conversionDateTime,
	toString(`ID`) AS conversionID,
	`OPPORTUNITY` AS conversionSum,
	'' AS yclid,
	'' as gclid,
	'' AS gaid,
	'' AS amiid,
	CASE
		WHEN LENGTH(`phone1`)<11 THEN ''
		ELSE CONCAT('7', SUBSTRING(replace(replace(replace(replace(replace(`phone1`, '(', ''), ')', ''), ' ', ''), '+', ''), '-', ''), 2))
	END AS phone,
	`email` AS email,
	'' AS google_aid,
	'' AS oaid,
	'' AS ios_ifa,
	'' AS ios_ifv, 
	'' AS windows_aid
FROM
	raw_bx_crm_lead_uf
*/
/* Битрикс24 (сделки) */
/*
UNION ALL
SELECT
	'deal' AS conversionType,
	'bitrix24' AS conversionSource,
	'' AS conversionSourceName,
	`DATE_CREATE` AS conversionDateTime,
	toString(`ID`) AS conversionID,
	`OPPORTUNITY` AS conversionSum,
	'' AS yclid,
	'' as gclid,
	'' AS gaid,
	'' AS amiid,
	CASE
		WHEN LENGTH(`phone1`)<11 THEN ''
		ELSE CONCAT('7', SUBSTRING(replace(replace(replace(replace(replace(`phone1`, '(', ''), ')', ''), ' ', ''), '+', ''), '-', ''), 2))
	END AS phone,
	`email` AS email,
	'' AS google_aid,
	'' AS oaid,
	'' AS ios_ifa,
	'' AS ios_ifv, 
	'' AS windows_aid
FROM
	raw_bx_crm_deal_uf
*/
/* Битрикс24 (продажи) */
/*
UNION ALL
SELECT
	'sale' AS conversionType,
	'bitrix24' AS conversionSource,
	'' AS conversionSourceName,
	`CLOSEDATE` AS conversionDateTime,
	toString(`ID`) AS conversionID,
	`OPPORTUNITY` AS conversionSum,
	'' AS yclid,
	'' as gclid,
	'' AS gaid,
	'' AS amiid,
	CASE
		WHEN LENGTH(`phone1`)<11 THEN ''
		ELSE CONCAT('7', SUBSTRING(replace(replace(replace(replace(replace(`phone1`, '(', ''), ')', ''), ' ', ''), '+', ''), '-', ''), 2))
	END AS phone,
	`email` AS email,
	'' AS google_aid,
	'' AS oaid,
	'' AS ios_ifa,
	'' AS ios_ifv, 
	'' AS windows_aid
FROM
	raw_bx_crm_deal_uf
WHERE
	(POSITION(`STAGE_ID`, ':')>0 AND SUBSTRING(`STAGE_ID`, LENGTH(`STAGE_ID`)-POSITION(REVERSE(`STAGE_ID`), ':')+2, LENGTH(`STAGE_ID`))='WON') OR `STAGE_ID`='WON'
*/
/* Битрикс (заказы) */
/*
UNION ALL
SELECT
	'order' AS conversionType,
	'bitrix' AS conversionSource,
	'' AS conversionSourceName,
	`dateInsert` AS conversionDateTime,
	toString(`id`) AS conversionID,
	`price` AS conversionSum,
	'' AS yclid,
	'' as gclid,
	'' AS gaid,
	'' AS amiid,
	'' AS phone,
	'' AS email,
	'' AS google_aid,
	'' AS oaid,
	'' AS ios_ifa,
	'' AS ios_ifv, 
	'' AS windows_aid
FROM
	raw_bx_orders
*/
/* Битрикс (продажи) */
/*
UNION ALL
SELECT
	'sale' AS conversionType,
	'bitrix' AS conversionSource,
	'' AS conversionSourceName,
	`dateInsert` AS conversionDateTime,
	toString(`id`) AS conversionID,
	`price` AS conversionSum,
	'' AS yclid,
	'' as gclid,
	'' AS gaid,
	'' AS amiid,
	'' AS phone,
	'' AS email,
	'' AS google_aid,
	'' AS oaid,
	'' AS ios_ifa,
	'' AS ios_ifv, 
	'' AS windows_aid
FROM
	raw_bx_orders
WHERE
	`payed`='Y' AND canceled='N'
*/