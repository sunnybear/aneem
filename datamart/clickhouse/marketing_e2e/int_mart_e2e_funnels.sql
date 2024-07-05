/* Цепочки касаний до конверсий (по всем типам конверсий) - на глубину 3, 7, 30, 90, 365 дней или всю */
/*
	int_mart_e2e_funnels_3days
	int_mart_e2e_funnels_week
	int_mart_e2e_funnels_month
	int_mart_e2e_funnels_quarter
	int_mart_e2e_funnels_year
	int_mart_e2e_funnels_any
*/
/*
	Тип конверсии: conversionType,
	Дата конверсии: conversionDateTime,
	ID конверсии (в источнике): conversionID,
	Источник конверсии: conversionSource,
	Название в источнике конверсии: conversionSourceName,
	Сумма конверсии: conversionSum,
	Телефон: phone,
	Email: email,
	Тип касания: touchType,
	Дата касания: touchDateTime,
	ID касания (в источнике): touchID,
	Источник касания: touchSource,
	Название в источнике касания: touchSourceName,
	Канал касания: UTMMedium,
	Источник касания: UTMSource,
	Кампания касания: UTMCampaign,
	Ключевое слово касания: UTMTerm,
	Содержание касания: UTMContent */
/* на глубину 3 дня */
CREATE OR REPLACE VIEW int_mart_e2e_funnels_3days AS
SELECT
    conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email,
    touchType,
    touchSource,
    touchSourceName,
    touchDateTime,
    touchID,
    UTMMedium,
    UTMSource,
    UTMCampaign,
    UTMTerm,
    UTMContent
FROM
    int_mart_e2e_conversions as c
LEFT JOIN
    int_mart_e2e_touches_phone AS t ON t.phone=c.phone
WHERE
    conversionDateTime>=touchDateTime
    AND conversionDateTime-touchDateTime<3*86400
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email,
    touchType,
    touchSource,
    touchSourceName,
    touchDateTime,
    touchID,
    UTMMedium,
    UTMSource,
    UTMCampaign,
    UTMTerm,
    UTMContent;
/* на глубину неделя */
CREATE OR REPLACE VIEW int_mart_e2e_funnels_week AS
SELECT
    conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email,
    touchType,
    touchSource,
    touchSourceName,
    touchDateTime,
    touchID,
    UTMMedium,
    UTMSource,
    UTMCampaign,
    UTMTerm,
    UTMContent
FROM
    int_mart_e2e_conversions as c
LEFT JOIN
    int_mart_e2e_touches_phone AS t ON t.phone=c.phone
WHERE
    conversionDateTime>=touchDateTime
    AND conversionDateTime-touchDateTime<7*86400
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email,
    touchType,
    touchSource,
    touchSourceName,
    touchDateTime,
    touchID,
    UTMMedium,
    UTMSource,
    UTMCampaign,
    UTMTerm,
    UTMContent;
/* на глубину месяц */
CREATE OR REPLACE VIEW int_mart_e2e_funnels_month AS
SELECT
    conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email,
    touchType,
    touchSource,
    touchSourceName,
    touchDateTime,
    touchID,
    UTMMedium,
    UTMSource,
    UTMCampaign,
    UTMTerm,
    UTMContent
FROM
    int_mart_e2e_conversions as c
LEFT JOIN
    int_mart_e2e_touches_phone AS t ON t.phone=c.phone
WHERE
    conversionDateTime>=touchDateTime
    AND conversionDateTime-touchDateTime<30*86400
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email,
    touchType,
    touchDateTime,
    touchID,
	touchSource,
	touchSourceName,
    UTMMedium,
    UTMSource,
    UTMCampaign,
    UTMTerm,
    UTMContent;
/* на глубину квартал */
CREATE OR REPLACE VIEW int_mart_e2e_funnels_quarter AS
SELECT
    conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email,
    touchType,
    touchSource,
    touchSourceName,
    touchDateTime,
    touchID,
    UTMMedium,
    UTMSource,
    UTMCampaign,
    UTMTerm,
    UTMContent
FROM
    int_mart_e2e_conversions as c
LEFT JOIN
    int_mart_e2e_touches_phone AS t ON t.phone=c.phone
WHERE
    conversionDateTime>=touchDateTime
    AND conversionDateTime-touchDateTime<90*86400
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email,
    touchType,
    touchDateTime,
    touchID,
	touchSource,
	touchSourceName,
    UTMMedium,
    UTMSource,
    UTMCampaign,
    UTMTerm,
    UTMContent;
/* на глубину год */
CREATE OR REPLACE VIEW int_mart_e2e_funnels_year AS
SELECT
    conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email,
    touchType,
    touchSource,
    touchSourceName,
    touchDateTime,
    touchID,
    UTMMedium,
    UTMSource,
    UTMCampaign,
    UTMTerm,
    UTMContent
FROM
    int_mart_e2e_conversions as c
LEFT JOIN
    int_mart_e2e_touches_phone AS t ON t.phone=c.phone
WHERE
    conversionDateTime>=touchDateTime
    AND conversionDateTime-touchDateTime<365*86400
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email,
    touchType,
    touchDateTime,
    touchID,
	touchSource,
	touchSourceName,
    UTMMedium,
    UTMSource,
    UTMCampaign,
    UTMTerm,
    UTMContent;
/* на всю глубину */
CREATE OR REPLACE VIEW int_mart_e2e_funnels_any AS
SELECT
    conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email,
    touchType,
    touchSource,
    touchSourceName,
    touchDateTime,
    touchID,
    UTMMedium,
    UTMSource,
    UTMCampaign,
    UTMTerm,
    UTMContent
FROM
    int_mart_e2e_conversions as c
LEFT JOIN
    int_mart_e2e_touches_phone AS t ON t.phone=c.phone
WHERE
    conversionDateTime>=touchDateTime
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email,
    touchType,
    touchDateTime,
    touchID,
	touchSource,
	touchSourceName,
    UTMMedium,
    UTMSource,
    UTMCampaign,
    UTMTerm,
    UTMContent;