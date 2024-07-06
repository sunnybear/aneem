/* Конверсии с атрибуцией по LPC (последнее платное касание) */
/* Платные каналы: cpc, cpm, ad, sms, media, affiliate, partners */
/*
	final_mart_e2e_lpc_3days
	final_mart_e2e_lpc_week
	final_mart_e2e_lpc_month
	final_mart_e2e_lpc_quarter
	final_mart_e2e_lpc_year
*/
/*
	Тип конверсии: conversionType,
	Дата конверсии: conversionDateTime,
	ID конверсии (в источнике): conversionID,
	Источник конверсии: conversionSource,
	Название в источнике конверсии: conversionSourceName,
	Вес конверсии (1): conversionWeight,
	Сумма конверсии: conversionSum,
	Телефон: phone,
	Email: email,
	Канал LPC: UTMMedium,
	Источник LPC: UTMSource,
	Кампания LPC: UTMCampaign,
	Ключевое слово LPC: UTMTerm,
	Содержание LPC: UTMContent */
/* на глубину 3 дня */
CREATE OR REPLACE VIEW final_mart_e2e_lpc_3days AS
SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	1 AS conversionWeight,
	conversionSum,
    phone,
	email,
    argMin(_UTMMedium, (conversionPriority)) AS UTMMedium,
    argMin(_UTMSource, (conversionPriority)) AS UTMSource,
	argMin(_UTMCampaign, (conversionPriority)) AS UTMCampaign,
    argMin(_UTMTerm, (conversionPriority)) AS UTMTerm,
	argMin(_UTMContent, (conversionPriority)) AS UTMContent
FROM (
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
	1 AS conversionPriority,
    phone,
	email,
    argMax(UTMMedium, (touchDateTime)) AS _UTMMedium,
    argMax(UTMSource, (touchDateTime)) AS _UTMSource,
	argMax(UTMCampaign, (touchDateTime)) AS _UTMCampaign,
    argMax(UTMTerm, (touchDateTime)) AS _UTMTerm,
	argMax(UTMContent, (touchDateTime)) AS _UTMContent
FROM
	int_mart_e2e_funnels_3days
WHERE
	UTMMedium IN ('cpc', 'cpm', 'ad', 'sms', 'affiliate', 'media', 'partners')
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email

UNION ALL

SELECT
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
	2 AS conversionPriority,
    phone,
	email,
    argMax(UTMMedium, (touchDateTime)) AS _UTMMedium,
    argMax(UTMSource, (touchDateTime)) AS _UTMSource,
	argMax(UTMCampaign, (touchDateTime)) AS _UTMCampaign,
    argMax(UTMTerm, (touchDateTime)) AS _UTMTerm,
	argMax(UTMContent, (touchDateTime)) AS _UTMContent
FROM
	int_mart_e2e_funnels_3days
WHERE
	UTMMedium NOT IN ('cpc', 'cpm', 'ad', 'sms', 'affiliate', 'media', 'partners')
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email)
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email;
/* на глубину неделя */
CREATE OR REPLACE VIEW final_mart_e2e_lndc_week AS
SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	1 AS conversionWeight,
	conversionSum,
    phone,
	email,
    argMin(_UTMMedium, (conversionPriority)) AS UTMMedium,
    argMin(_UTMSource, (conversionPriority)) AS UTMSource,
	argMin(_UTMCampaign, (conversionPriority)) AS UTMCampaign,
    argMin(_UTMTerm, (conversionPriority)) AS UTMTerm,
	argMin(_UTMContent, (conversionPriority)) AS UTMContent
FROM (
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
	1 AS conversionPriority,
    phone,
	email,
    argMax(UTMMedium, (touchDateTime)) AS _UTMMedium,
    argMax(UTMSource, (touchDateTime)) AS _UTMSource,
	argMax(UTMCampaign, (touchDateTime)) AS _UTMCampaign,
    argMax(UTMTerm, (touchDateTime)) AS _UTMTerm,
	argMax(UTMContent, (touchDateTime)) AS _UTMContent
FROM
	int_mart_e2e_funnels_week
WHERE
	UTMMedium IN ('cpc', 'cpm', 'ad', 'sms', 'affiliate', 'media', 'partners')
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email

UNION ALL

SELECT
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
	2 AS conversionPriority,
    phone,
	email,
    argMax(UTMMedium, (touchDateTime)) AS _UTMMedium,
    argMax(UTMSource, (touchDateTime)) AS _UTMSource,
	argMax(UTMCampaign, (touchDateTime)) AS _UTMCampaign,
    argMax(UTMTerm, (touchDateTime)) AS _UTMTerm,
	argMax(UTMContent, (touchDateTime)) AS _UTMContent
FROM
	int_mart_e2e_funnels_week
WHERE
	UTMMedium NOT IN ('cpc', 'cpm', 'ad', 'sms', 'affiliate', 'media', 'partners')
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email)
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email;
/* на глубину месяц */
CREATE OR REPLACE VIEW final_mart_e2e_lndc_month AS
SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	1 AS conversionWeight,
	conversionSum,
    phone,
	email,
    argMin(_UTMMedium, (conversionPriority)) AS UTMMedium,
    argMin(_UTMSource, (conversionPriority)) AS UTMSource,
	argMin(_UTMCampaign, (conversionPriority)) AS UTMCampaign,
    argMin(_UTMTerm, (conversionPriority)) AS UTMTerm,
	argMin(_UTMContent, (conversionPriority)) AS UTMContent
FROM (
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
	1 AS conversionPriority,
    phone,
	email,
    argMax(UTMMedium, (touchDateTime)) AS _UTMMedium,
    argMax(UTMSource, (touchDateTime)) AS _UTMSource,
	argMax(UTMCampaign, (touchDateTime)) AS _UTMCampaign,
    argMax(UTMTerm, (touchDateTime)) AS _UTMTerm,
	argMax(UTMContent, (touchDateTime)) AS _UTMContent
FROM
	int_mart_e2e_funnels_month
WHERE
	UTMMedium IN ('cpc', 'cpm', 'ad', 'sms', 'affiliate', 'media', 'partners')
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email

UNION ALL

SELECT
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
	2 AS conversionPriority,
    phone,
	email,
    argMax(UTMMedium, (touchDateTime)) AS _UTMMedium,
    argMax(UTMSource, (touchDateTime)) AS _UTMSource,
	argMax(UTMCampaign, (touchDateTime)) AS _UTMCampaign,
    argMax(UTMTerm, (touchDateTime)) AS _UTMTerm,
	argMax(UTMContent, (touchDateTime)) AS _UTMContent
FROM
	int_mart_e2e_funnels_month
WHERE
	UTMMedium NOT IN ('cpc', 'cpm', 'ad', 'sms', 'affiliate', 'media', 'partners')
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email)
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email;
/* на глубину квартал */
CREATE OR REPLACE VIEW final_mart_e2e_lndc_quarter AS
SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	1 AS conversionWeight,
	conversionSum,
    phone,
	email,
    argMin(_UTMMedium, (conversionPriority)) AS UTMMedium,
    argMin(_UTMSource, (conversionPriority)) AS UTMSource,
	argMin(_UTMCampaign, (conversionPriority)) AS UTMCampaign,
    argMin(_UTMTerm, (conversionPriority)) AS UTMTerm,
	argMin(_UTMContent, (conversionPriority)) AS UTMContent
FROM (
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
	1 AS conversionPriority,
    phone,
	email,
    argMax(UTMMedium, (touchDateTime)) AS _UTMMedium,
    argMax(UTMSource, (touchDateTime)) AS _UTMSource,
	argMax(UTMCampaign, (touchDateTime)) AS _UTMCampaign,
    argMax(UTMTerm, (touchDateTime)) AS _UTMTerm,
	argMax(UTMContent, (touchDateTime)) AS _UTMContent
FROM
	int_mart_e2e_funnels_quarter
WHERE
	UTMMedium IN ('cpc', 'cpm', 'ad', 'sms', 'affiliate', 'media', 'partners')
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email

UNION ALL

SELECT
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
	2 AS conversionPriority,
    phone,
	email,
    argMax(UTMMedium, (touchDateTime)) AS _UTMMedium,
    argMax(UTMSource, (touchDateTime)) AS _UTMSource,
	argMax(UTMCampaign, (touchDateTime)) AS _UTMCampaign,
    argMax(UTMTerm, (touchDateTime)) AS _UTMTerm,
	argMax(UTMContent, (touchDateTime)) AS _UTMContent
FROM
	int_mart_e2e_funnels_quarter
WHERE
	UTMMedium NOT IN ('cpc', 'cpm', 'ad', 'sms', 'affiliate', 'media', 'partners')
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email)
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email;
/* на глубину год */
CREATE OR REPLACE VIEW final_mart_e2e_lndc_year AS
SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	1 AS conversionWeight,
	conversionSum,
    phone,
	email,
    argMin(_UTMMedium, (conversionPriority)) AS UTMMedium,
    argMin(_UTMSource, (conversionPriority)) AS UTMSource,
	argMin(_UTMCampaign, (conversionPriority)) AS UTMCampaign,
    argMin(_UTMTerm, (conversionPriority)) AS UTMTerm,
	argMin(_UTMContent, (conversionPriority)) AS UTMContent
FROM (
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
	1 AS conversionPriority,
    phone,
	email,
    argMax(UTMMedium, (touchDateTime)) AS _UTMMedium,
    argMax(UTMSource, (touchDateTime)) AS _UTMSource,
	argMax(UTMCampaign, (touchDateTime)) AS _UTMCampaign,
    argMax(UTMTerm, (touchDateTime)) AS _UTMTerm,
	argMax(UTMContent, (touchDateTime)) AS _UTMContent
FROM
	int_mart_e2e_funnels_year
WHERE
	UTMMedium IN ('cpc', 'cpm', 'ad', 'sms', 'affiliate', 'media', 'partners')
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email

UNION ALL

SELECT
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
	2 AS conversionPriority,
    phone,
	email,
    argMax(UTMMedium, (touchDateTime)) AS _UTMMedium,
    argMax(UTMSource, (touchDateTime)) AS _UTMSource,
	argMax(UTMCampaign, (touchDateTime)) AS _UTMCampaign,
    argMax(UTMTerm, (touchDateTime)) AS _UTMTerm,
	argMax(UTMContent, (touchDateTime)) AS _UTMContent
FROM
	int_mart_e2e_funnels_year
WHERE
	UTMMedium NOT IN ('cpc', 'cpm', 'ad', 'sms', 'affiliate', 'media', 'partners')
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email)
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email;