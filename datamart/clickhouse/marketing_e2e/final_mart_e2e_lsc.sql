/* Конверсии с атрибуцией по LSC (последнее значимое касание) */
/* Приоритет каналов для учета: affiliate, media -> cpc, cpm, ad -> sms, partners -> organic, social, smm, email, referral, offline, messenger -> <other> -> direct, internal */
/*
	final_mart_e2e_lsc_3days
	final_mart_e2e_lsc_week
	final_mart_e2e_lsc_month
	final_mart_e2e_lsc_quarter
	final_mart_e2e_lsc_year
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
	Канал LSC: UTMMedium,
	Источник LSC: UTMSource,
	Кампания LSC: UTMCampaign,
	Ключевое слово LSC: UTMTerm,
	Содержание LSC: UTMContent */
/* на глубину 3 дня */
CREATE OR REPLACE VIEW final_mart_e2e_lsc_3days AS
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
	UTMMedium IN ('affiliate', 'media')
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
	UTMMedium IN ('cpc', 'cpm', 'ad')
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
	3 AS conversionPriority,
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
	UTMMedium IN ('sms', 'partners')
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
	4 AS conversionPriority,
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
	UTMMedium IN ('organic', 'social', 'smm', 'email', 'referral', 'offline', 'messenger')
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
	5 AS conversionPriority,
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
	UTMMedium NOT IN ('affiliate', 'media', 'cpc', 'cpm', 'ad', 'sms', 'partners', 'organic', 'social', 'smm', 'email', 'referral', 'offline', 'messenger', 'direct', 'internal')
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
	6 AS conversionPriority,
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
	UTMMedium IN ('direct', 'internal')
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
CREATE OR REPLACE VIEW final_mart_e2e_lsc_week AS
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
	UTMMedium IN ('affiliate', 'media')
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
	UTMMedium IN ('cpc', 'cpm', 'ad')
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
	3 AS conversionPriority,
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
	UTMMedium IN ('sms', 'partners')
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
	4 AS conversionPriority,
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
	UTMMedium IN ('organic', 'social', 'smm', 'email', 'referral', 'offline', 'messenger')
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
	5 AS conversionPriority,
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
	UTMMedium NOT IN ('affiliate', 'media', 'cpc', 'cpm', 'ad', 'sms', 'partners', 'organic', 'social', 'smm', 'email', 'referral', 'offline', 'messenger', 'direct', 'internal')
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
	6 AS conversionPriority,
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
	UTMMedium IN ('direct', 'internal')
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
CREATE OR REPLACE VIEW final_mart_e2e_lsc_month AS
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
	UTMMedium IN ('affiliate', 'media')
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
	UTMMedium IN ('cpc', 'cpm', 'ad')
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
	3 AS conversionPriority,
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
	UTMMedium IN ('sms', 'partners')
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
	4 AS conversionPriority,
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
	UTMMedium IN ('organic', 'social', 'smm', 'email', 'referral', 'offline', 'messenger')
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
	5 AS conversionPriority,
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
	UTMMedium NOT IN ('affiliate', 'media', 'cpc', 'cpm', 'ad', 'sms', 'partners', 'organic', 'social', 'smm', 'email', 'referral', 'offline', 'messenger', 'direct', 'internal')
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
	6 AS conversionPriority,
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
	UTMMedium IN ('direct', 'internal')
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
CREATE OR REPLACE VIEW final_mart_e2e_lsc_quarter AS
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
	UTMMedium IN ('affiliate', 'media')
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
	UTMMedium IN ('cpc', 'cpm', 'ad')
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
	3 AS conversionPriority,
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
	UTMMedium IN ('sms', 'partners')
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
	4 AS conversionPriority,
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
	UTMMedium IN ('organic', 'social', 'smm', 'email', 'referral', 'offline', 'messenger')
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
	5 AS conversionPriority,
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
	UTMMedium NOT IN ('affiliate', 'media', 'cpc', 'cpm', 'ad', 'sms', 'partners', 'organic', 'social', 'smm', 'email', 'referral', 'offline', 'messenger', 'direct', 'internal')
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
	6 AS conversionPriority,
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
	UTMMedium IN ('direct', 'internal')
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
CREATE OR REPLACE VIEW final_mart_e2e_lsc_year AS
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
	UTMMedium IN ('affiliate', 'media')
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
	UTMMedium IN ('cpc', 'cpm', 'ad')
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
	3 AS conversionPriority,
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
	UTMMedium IN ('sms', 'partners')
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
	4 AS conversionPriority,
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
	UTMMedium IN ('organic', 'social', 'smm', 'email', 'referral', 'offline', 'messenger')
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
	5 AS conversionPriority,
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
	UTMMedium NOT IN ('affiliate', 'media', 'cpc', 'cpm', 'ad', 'sms', 'partners', 'organic', 'social', 'smm', 'email', 'referral', 'offline', 'messenger', 'direct', 'internal')
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
	6 AS conversionPriority,
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
	UTMMedium IN ('direct', 'internal')
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