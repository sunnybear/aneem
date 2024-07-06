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
-- 1. целевая таблица
CREATE OR REPLACE TABLE int_mart_e2e_funnels_3days (
	conversionType String,
    conversionDateTime DateTime,
	conversionID String,
	conversionSource String,
	conversionSourceName String,
	conversionSum Int64,
    phone String,
	email String,
    touchType String,
    touchSource String,
    touchSourceName String,
    touchDateTime DateTime,
    touchID String,
	UTMMedium String,
    UTMSource String,
	UTMCampaign String,
    UTMTerm String,
	UTMContent String
)
ENGINE = SummingMergeTree
ORDER BY (conversionType, conversionDateTime, conversionID, conversionSource, conversionSourceName, conversionSum, phone, email, touchType, touchSource, touchSourceName, touchDateTime, touchID, UTMMedium, UTMSource, UTMCampaign, UTMTerm, UTMContent);

-- 2. материализованное представление (триггер на обновление данных целевой таблицы)
DROP VIEW IF EXISTS int_mart_e2e_funnels_3days_mv;
CREATE MATERIALIZED VIEW int_mart_e2e_funnels_3days_mv TO int_mart_e2e_funnels_3days AS
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

-- 3. загрузка исходных данных
INSERT INTO int_mart_e2e_funnels_3days SELECT 
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
-- 1. целевая таблица
CREATE OR REPLACE TABLE int_mart_e2e_funnels_week (
	conversionType String,
    conversionDateTime DateTime,
	conversionID String,
	conversionSource String,
	conversionSourceName String,
	conversionSum Int64,
    phone String,
	email String,
    touchType String,
    touchSource String,
    touchSourceName String,
    touchDateTime DateTime,
    touchID String,
	UTMMedium String,
    UTMSource String,
	UTMCampaign String,
    UTMTerm String,
	UTMContent String
)
ENGINE = SummingMergeTree
ORDER BY (conversionType, conversionDateTime, conversionID, conversionSource, conversionSourceName, conversionSum, phone, email, touchType, touchSource, touchSourceName, touchDateTime, touchID, UTMMedium, UTMSource, UTMCampaign, UTMTerm, UTMContent);

-- 2. материализованное представление (триггер на обновление данных целевой таблицы)
DROP VIEW IF EXISTS int_mart_e2e_funnels_week_mv;
CREATE MATERIALIZED VIEW int_mart_e2e_funnels_week_mv TO int_mart_e2e_funnels_week AS
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

-- 3. загрузка исходных данных
INSERT INTO int_mart_e2e_funnels_week SELECT 
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
/* можно рассчитать отдельно по годам:	AND YEAR(conversionDateTime)=2024 */
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
-- 1. целевая таблица
CREATE OR REPLACE TABLE int_mart_e2e_funnels_month (
	conversionType String,
    conversionDateTime DateTime,
	conversionID String,
	conversionSource String,
	conversionSourceName String,
	conversionSum Int64,
    phone String,
	email String,
    touchType String,
    touchSource String,
    touchSourceName String,
    touchDateTime DateTime,
    touchID String,
	UTMMedium String,
    UTMSource String,
	UTMCampaign String,
    UTMTerm String,
	UTMContent String
)
ENGINE = SummingMergeTree
ORDER BY (conversionType, conversionDateTime, conversionID, conversionSource, conversionSourceName, conversionSum, phone, email, touchType, touchSource, touchSourceName, touchDateTime, touchID, UTMMedium, UTMSource, UTMCampaign, UTMTerm, UTMContent);

-- 2. материализованное представление (триггер на обновление данных целевой таблицы)
DROP VIEW IF EXISTS int_mart_e2e_funnels_month_mv;
CREATE MATERIALIZED VIEW int_mart_e2e_funnels_month_mv TO int_mart_e2e_funnels_month AS
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
    touchSource,
    touchSourceName,
    touchDateTime,
    touchID,
    UTMMedium,
    UTMSource,
    UTMCampaign,
    UTMTerm,
    UTMContent;

-- 3. загрузка исходных данных
INSERT INTO int_mart_e2e_funnels_month SELECT 
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
/* можно рассчитать отдельно по годам:	AND YEAR(conversionDateTime)=2024 */
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
/* на глубину квартал */
-- 1. целевая таблица
CREATE OR REPLACE TABLE int_mart_e2e_funnels_quarter (
	conversionType String,
    conversionDateTime DateTime,
	conversionID String,
	conversionSource String,
	conversionSourceName String,
	conversionSum Int64,
    phone String,
	email String,
    touchType String,
    touchSource String,
    touchSourceName String,
    touchDateTime DateTime,
    touchID String,
	UTMMedium String,
    UTMSource String,
	UTMCampaign String,
    UTMTerm String,
	UTMContent String
)
ENGINE = SummingMergeTree
ORDER BY (conversionType, conversionDateTime, conversionID, conversionSource, conversionSourceName, conversionSum, phone, email, touchType, touchSource, touchSourceName, touchDateTime, touchID, UTMMedium, UTMSource, UTMCampaign, UTMTerm, UTMContent);

-- 2. материализованное представление (триггер на обновление данных целевой таблицы)
DROP VIEW IF EXISTS int_mart_e2e_funnels_quarter_mv;
CREATE MATERIALIZED VIEW int_mart_e2e_funnels_quarter_mv TO int_mart_e2e_funnels_quarter AS
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
    touchSource,
    touchSourceName,
    touchDateTime,
    touchID,
    UTMMedium,
    UTMSource,
    UTMCampaign,
    UTMTerm,
    UTMContent;

-- 3. загрузка исходных данных
INSERT INTO int_mart_e2e_funnels_quarter SELECT 
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
/* можно рассчитать отдельно по годам:	AND YEAR(conversionDateTime)=2024 */
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
/* на глубину год */
-- 1. целевая таблица
CREATE OR REPLACE TABLE int_mart_e2e_funnels_year (
	conversionType String,
    conversionDateTime DateTime,
	conversionID String,
	conversionSource String,
	conversionSourceName String,
	conversionSum Int64,
    phone String,
	email String,
    touchType String,
    touchSource String,
    touchSourceName String,
    touchDateTime DateTime,
    touchID String,
	UTMMedium String,
    UTMSource String,
	UTMCampaign String,
    UTMTerm String,
	UTMContent String
)
ENGINE = SummingMergeTree
ORDER BY (conversionType, conversionDateTime, conversionID, conversionSource, conversionSourceName, conversionSum, phone, email, touchType, touchSource, touchSourceName, touchDateTime, touchID, UTMMedium, UTMSource, UTMCampaign, UTMTerm, UTMContent);

-- 2. материализованное представление (триггер на обновление данных целевой таблицы)
DROP VIEW IF EXISTS int_mart_e2e_funnels_year_mv;
CREATE MATERIALIZED VIEW int_mart_e2e_funnels_year_mv TO int_mart_e2e_funnels_year AS
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
    touchSource,
    touchSourceName,
    touchDateTime,
    touchID,
    UTMMedium,
    UTMSource,
    UTMCampaign,
    UTMTerm,
    UTMContent;

-- 3. загрузка исходных данных
INSERT INTO int_mart_e2e_funnels_year SELECT 
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
/* можно рассчитать отдельно по годам:	AND YEAR(conversionDateTime)=2024 */
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
/* на всю глубину */
-- 1. целевая таблица
CREATE OR REPLACE TABLE int_mart_e2e_funnels_any (
	conversionType String,
    conversionDateTime DateTime,
	conversionID String,
	conversionSource String,
	conversionSourceName String,
	conversionSum Int64,
    phone String,
	email String,
    touchType String,
    touchSource String,
    touchSourceName String,
    touchDateTime DateTime,
    touchID String,
	UTMMedium String,
    UTMSource String,
	UTMCampaign String,
    UTMTerm String,
	UTMContent String
)
ENGINE = SummingMergeTree
ORDER BY (conversionType, conversionDateTime, conversionID, conversionSource, conversionSourceName, conversionSum, phone, email, touchType, touchSource, touchSourceName, touchDateTime, touchID, UTMMedium, UTMSource, UTMCampaign, UTMTerm, UTMContent);

-- 2. материализованное представление (триггер на обновление данных целевой таблицы)
DROP VIEW IF EXISTS int_mart_e2e_funnels_any_mv;
CREATE MATERIALIZED VIEW int_mart_e2e_funnels_any_mv TO int_mart_e2e_funnels_any AS
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
    touchSource,
    touchSourceName,
    touchDateTime,
    touchID,
    UTMMedium,
    UTMSource,
    UTMCampaign,
    UTMTerm,
    UTMContent;

-- 3. загрузка исходных данных
INSERT INTO int_mart_e2e_funnels_any SELECT 
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
/* можно рассчитать отдельно по годам:	AND YEAR(conversionDateTime)=2024 */
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