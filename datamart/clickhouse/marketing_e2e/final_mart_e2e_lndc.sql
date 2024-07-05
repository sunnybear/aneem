/* Конверсии с атрибуцией по LNDC (последнее непрямое касание) */
/*
	final_mart_e2e_lndc_3days
	final_mart_e2e_lndc_week
	final_mart_e2e_lndc_month
	final_mart_e2e_lndc_quarter
	final_mart_e2e_lndc_year
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
	Канал LNDC: UTMMedium,
	Источник LNDC: UTMSource,
	Кампания LNDC: UTMCampaign,
	Ключевое слово LNDC: UTMTerm,
	Содержание LNDC: UTMContent */
/* на глубину 3 дня */
CREATE OR REPLACE VIEW final_mart_e2e_lndc_3days AS
SELECT 
	conversionType,
	CASE
		WHEN _UTMMedium='direct' THEN conversionDateTime-1
		ELSE conversionDateTime
    END AS conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	1 AS conversionWeight,
	conversionSum,
    phone,
	email,
    argMax(_UTMMedium, (conversionDateTime)) AS UTMMedium,
    argMax(_UTMSource, (conversionDateTime)) AS UTMSource,
	argMax(_UTMCampaign, (conversionDateTime)) AS UTMCampaign,
    argMax(_UTMTerm, (conversionDateTime)) AS UTMTerm,
	argMax(_UTMContent, (conversionDateTime)) AS UTMContent
FROM (
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
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
	UTMMedium NOT IN ('direct', 'internal', 'Веб-сайт')
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
    conversionDateTime+1,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email,
    'direct',
    argMax(UTMSource, (touchDateTime)),
	'',
    '',
	''
FROM
	int_mart_e2e_funnels_3days
WHERE
	UTMMedium IN ('direct', 'internal', 'Веб-сайт')
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
	CASE
		WHEN _UTMMedium='direct' THEN conversionDateTime-1
		ELSE conversionDateTime
    END AS conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	1 AS conversionWeight,
	conversionSum,
    phone,
	email,
    argMax(_UTMMedium, (conversionDateTime)) AS UTMMedium,
    argMax(_UTMSource, (conversionDateTime)) AS UTMSource,
	argMax(_UTMCampaign, (conversionDateTime)) AS UTMCampaign,
    argMax(_UTMTerm, (conversionDateTime)) AS UTMTerm,
	argMax(_UTMContent, (conversionDateTime)) AS UTMContent
FROM (
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
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
	UTMMedium NOT IN ('direct', 'internal', 'Веб-сайт')
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
    conversionDateTime+1,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email,
    'direct',
    argMax(UTMSource, (touchDateTime)),
	'',
    '',
	''
FROM
	int_mart_e2e_funnels_week
WHERE
	UTMMedium IN ('direct', 'internal', 'Веб-сайт')
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
	CASE
		WHEN _UTMMedium='direct' THEN conversionDateTime-1
		ELSE conversionDateTime
    END AS conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	1 AS conversionWeight,
	conversionSum,
    phone,
	email,
    argMax(_UTMMedium, (conversionDateTime)) AS UTMMedium,
    argMax(_UTMSource, (conversionDateTime)) AS UTMSource,
	argMax(_UTMCampaign, (conversionDateTime)) AS UTMCampaign,
    argMax(_UTMTerm, (conversionDateTime)) AS UTMTerm,
	argMax(_UTMContent, (conversionDateTime)) AS UTMContent
FROM (
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
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
	UTMMedium NOT IN ('direct', 'internal', 'Веб-сайт')
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
    conversionDateTime+1,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email,
    'direct',
    argMax(UTMSource, (touchDateTime)),
	'',
    '',
	''
FROM
	int_mart_e2e_funnels_month
WHERE
	UTMMedium IN ('direct', 'internal', 'Веб-сайт')
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
	CASE
		WHEN _UTMMedium='direct' THEN conversionDateTime-1
		ELSE conversionDateTime
    END AS conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	1 AS conversionWeight,
	conversionSum,
    phone,
	email,
    argMax(_UTMMedium, (conversionDateTime)) AS UTMMedium,
    argMax(_UTMSource, (conversionDateTime)) AS UTMSource,
	argMax(_UTMCampaign, (conversionDateTime)) AS UTMCampaign,
    argMax(_UTMTerm, (conversionDateTime)) AS UTMTerm,
	argMax(_UTMContent, (conversionDateTime)) AS UTMContent
FROM (
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
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
	UTMMedium NOT IN ('direct', 'internal', 'Веб-сайт')
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
    conversionDateTime+1,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email,
    'direct',
    argMax(UTMSource, (touchDateTime)),
	'',
    '',
	''
FROM
	int_mart_e2e_funnels_quarter
WHERE
	UTMMedium IN ('direct', 'internal', 'Веб-сайт')
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
	CASE
		WHEN _UTMMedium='direct' THEN conversionDateTime-1
		ELSE conversionDateTime
    END AS conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	1 AS conversionWeight,
	conversionSum,
    phone,
	email,
    argMax(_UTMMedium, (conversionDateTime)) AS UTMMedium,
    argMax(_UTMSource, (conversionDateTime)) AS UTMSource,
	argMax(_UTMCampaign, (conversionDateTime)) AS UTMCampaign,
    argMax(_UTMTerm, (conversionDateTime)) AS UTMTerm,
	argMax(_UTMContent, (conversionDateTime)) AS UTMContent
FROM (
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
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
	UTMMedium NOT IN ('direct', 'internal', 'Веб-сайт')
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
    conversionDateTime+1,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email,
    'direct',
    argMax(UTMSource, (touchDateTime)),
	'',
    '',
	''
FROM
	int_mart_e2e_funnels_year
WHERE
	UTMMedium IN ('direct', 'internal', 'Веб-сайт')
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