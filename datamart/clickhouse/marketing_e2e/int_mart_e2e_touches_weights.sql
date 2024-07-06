/* Касания до конверсий с весами и порядковыми номерами - нужны для много-касательных атрибуций */
/*
	int_mart_e2e_touches_3days
	int_mart_e2e_touches_week
	int_mart_e2e_touches_month
	int_mart_e2e_touches_quarter
	int_mart_e2e_touches_year
	int_mart_e2e_touches_any
*/
/*
	Тип конверсии: conversionType,
	Дата конверсии: conversionDateTime,
	ID конверсии (в источнике): conversionID,
	Источник конверсии: conversionSource,
	Название в источнике конверсии: conversionSourceName,
	Вес касания: touchWeight,
	Веса всех касаний конверсии: touchWeights,
	Номер касания по дате: touchNumber,
	Сумма конверсии: _conversionSum,
	Телефон: phone,
	Email: email,
	Канал касания: _UTMMedium,
	Источник касания: _UTMSource,
	Кампания касания: _UTMCampaign,
	Ключевое слово касания: _UTMTerm,
	Содержание касания: _UTMContent */
/* на глубину 3 дня */
-- 1. целевая таблица
CREATE OR REPLACE TABLE int_mart_e2e_touches_3days
(
	conversionType String,
    conversionDateTime DateTime,
	conversionID String,
	conversionSource String,
	conversionSourceName String,
	_conversionSum Int64,
	touchWeight Float64,
	touchWeights Int64,
	touchNumber Int64,
    phone String,
	email String,
	_UTMMedium String,
    _UTMSource String,
	_UTMCampaign String,
    _UTMTerm String,
	_UTMContent String
)
ENGINE = SummingMergeTree
ORDER BY (conversionType, conversionDateTime, conversionID, conversionSource, conversionSourceName, _conversionSum, touchWeight, touchWeights, phone, email, _UTMMedium, _UTMSource, _UTMCampaign, _UTMTerm, _UTMContent);

-- 2. материализованное представление (триггер на обновление данных целевой таблицы)
DROP VIEW IF EXISTS int_mart_e2e_touches_3days_mv;
CREATE MATERIALIZED VIEW int_mart_e2e_touches_3days_mv TO int_mart_e2e_touches_3days AS
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum AS _conversionSum,
	CASE
		WHEN UTMMedium='direct' THEN 0.000000000001
		WHEN UTMMedium='internal' THEN 0.000000000001
		WHEN UTMMedium='Веб-сайт' THEN 0.000000000001
		ELSE 1.0
	END AS touchWeight,
	(sum(touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)) AS touchWeights,
	(row_number() OVER (PARTITION BY conversionType,conversionID,conversionSource ORDER BY touchDateTime)) AS touchNumber,
    phone,
	email,
	CASE
		WHEN UTMMedium='internal' THEN 'direct'
		WHEN UTMMedium='Веб-сайт' THEN 'direct'
		ELSE UTMMedium
	END AS _UTMMedium,
    UTMSource AS _UTMSource,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMCampaign
	END AS _UTMCampaign,
    CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMTerm
	END AS _UTMTerm,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMContent
	END AS _UTMContent
FROM
	int_mart_e2e_funnels_3days;

-- 3. загрузка исходных данных
CREATE OR REPLACE VIEW int_mart_e2e_touches_3days AS
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum AS _conversionSum,
	CASE
		WHEN UTMMedium='direct' THEN 0.000000000001
		WHEN UTMMedium='internal' THEN 0.000000000001
		WHEN UTMMedium='Веб-сайт' THEN 0.000000000001
		ELSE 1.0
	END AS touchWeight,
	(sum(touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)) AS touchWeights,
	(row_number() OVER (PARTITION BY conversionType,conversionID,conversionSource ORDER BY touchDateTime)) AS touchNumber,
    phone,
	email,
	CASE
		WHEN UTMMedium='internal' THEN 'direct'
		WHEN UTMMedium='Веб-сайт' THEN 'direct'
		ELSE UTMMedium
	END AS _UTMMedium,
    UTMSource AS _UTMSource,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMCampaign
	END AS _UTMCampaign,
    CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMTerm
	END AS _UTMTerm,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMContent
	END AS _UTMContent
FROM
	int_mart_e2e_funnels_3days;
/* на глубину неделя */
-- 1. целевая таблица
CREATE OR REPLACE TABLE int_mart_e2e_touches_week
(
	conversionType String,
    conversionDateTime DateTime,
	conversionID String,
	conversionSource String,
	conversionSourceName String,
	_conversionSum Int64,
	touchWeight Float64,
	touchWeights Int64,
	touchNumber Int64,
    phone String,
	email String,
	_UTMMedium String,
    _UTMSource String,
	_UTMCampaign String,
    _UTMTerm String,
	_UTMContent String
)
ENGINE = SummingMergeTree
ORDER BY (conversionType, conversionDateTime, conversionID, conversionSource, conversionSourceName, _conversionSum, touchWeight, touchWeights, phone, email, _UTMMedium, _UTMSource, _UTMCampaign, _UTMTerm, _UTMContent);

-- 2. материализованное представление (триггер на обновление данных целевой таблицы)
DROP VIEW IF EXISTS int_mart_e2e_touches_week_mv;
CREATE MATERIALIZED VIEW int_mart_e2e_touches_week_mv TO int_mart_e2e_touches_week AS
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum AS _conversionSum,
	CASE
		WHEN UTMMedium='direct' THEN 0.000000000001
		WHEN UTMMedium='internal' THEN 0.000000000001
		WHEN UTMMedium='Веб-сайт' THEN 0.000000000001
		ELSE 1.0
	END AS touchWeight,
	(sum(touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)) AS touchWeights,
	(row_number() OVER (PARTITION BY conversionType,conversionID,conversionSource ORDER BY touchDateTime)) AS touchNumber,
    phone,
	email,
	CASE
		WHEN UTMMedium='internal' THEN 'direct'
		WHEN UTMMedium='Веб-сайт' THEN 'direct'
		ELSE UTMMedium
	END AS _UTMMedium,
    UTMSource AS _UTMSource,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMCampaign
	END AS _UTMCampaign,
    CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMTerm
	END AS _UTMTerm,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMContent
	END AS _UTMContent
FROM
	int_mart_e2e_funnels_week;

-- 3. загрузка исходных данных
CREATE OR REPLACE VIEW int_mart_e2e_touches_week AS
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum AS _conversionSum,
	CASE
		WHEN UTMMedium='direct' THEN 0.000000000001
		WHEN UTMMedium='internal' THEN 0.000000000001
		WHEN UTMMedium='Веб-сайт' THEN 0.000000000001
		ELSE 1.0
	END AS touchWeight,
	(sum(touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)) AS touchWeights,
	(row_number() OVER (PARTITION BY conversionType,conversionID,conversionSource ORDER BY touchDateTime)) AS touchNumber,
    phone,
	email,
	CASE
		WHEN UTMMedium='internal' THEN 'direct'
		WHEN UTMMedium='Веб-сайт' THEN 'direct'
		ELSE UTMMedium
	END AS _UTMMedium,
    UTMSource AS _UTMSource,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMCampaign
	END AS _UTMCampaign,
    CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMTerm
	END AS _UTMTerm,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMContent
	END AS _UTMContent
FROM
	int_mart_e2e_funnels_week;
/* на глубину месяц */
-- 1. целевая таблица
CREATE OR REPLACE TABLE int_mart_e2e_touches_month
(
	conversionType String,
    conversionDateTime DateTime,
	conversionID String,
	conversionSource String,
	conversionSourceName String,
	_conversionSum Int64,
	touchWeight Float64,
	touchWeights Int64,
	touchNumber Int64,
    phone String,
	email String,
	_UTMMedium String,
    _UTMSource String,
	_UTMCampaign String,
    _UTMTerm String,
	_UTMContent String
)
ENGINE = SummingMergeTree
ORDER BY (conversionType, conversionDateTime, conversionID, conversionSource, conversionSourceName, _conversionSum, touchWeight, touchWeights, phone, email, _UTMMedium, _UTMSource, _UTMCampaign, _UTMTerm, _UTMContent);

-- 2. материализованное представление (триггер на обновление данных целевой таблицы)
DROP VIEW IF EXISTS int_mart_e2e_touches_month_mv;
CREATE MATERIALIZED VIEW int_mart_e2e_touches_month_mv TO int_mart_e2e_touches_month AS
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum AS _conversionSum,
	CASE
		WHEN UTMMedium='direct' THEN 0.000000000001
		WHEN UTMMedium='internal' THEN 0.000000000001
		WHEN UTMMedium='Веб-сайт' THEN 0.000000000001
		ELSE 1.0
	END AS touchWeight,
	(sum(touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)) AS touchWeights,
	(row_number() OVER (PARTITION BY conversionType,conversionID,conversionSource ORDER BY touchDateTime)) AS touchNumber,
    phone,
	email,
	CASE
		WHEN UTMMedium='internal' THEN 'direct'
		WHEN UTMMedium='Веб-сайт' THEN 'direct'
		ELSE UTMMedium
	END AS _UTMMedium,
    UTMSource AS _UTMSource,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMCampaign
	END AS _UTMCampaign,
    CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMTerm
	END AS _UTMTerm,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMContent
	END AS _UTMContent
FROM
	int_mart_e2e_funnels_month;

-- 3. загрузка исходных данных
CREATE OR REPLACE VIEW int_mart_e2e_touches_month AS
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum AS _conversionSum,
	CASE
		WHEN UTMMedium='direct' THEN 0.000000000001
		WHEN UTMMedium='internal' THEN 0.000000000001
		WHEN UTMMedium='Веб-сайт' THEN 0.000000000001
		ELSE 1.0
	END AS touchWeight,
	(sum(touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)) AS touchWeights,
	(row_number() OVER (PARTITION BY conversionType,conversionID,conversionSource ORDER BY touchDateTime)) AS touchNumber,
    phone,
	email,
	CASE
		WHEN UTMMedium='internal' THEN 'direct'
		WHEN UTMMedium='Веб-сайт' THEN 'direct'
		ELSE UTMMedium
	END AS _UTMMedium,
    UTMSource AS _UTMSource,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMCampaign
	END AS _UTMCampaign,
    CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMTerm
	END AS _UTMTerm,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMContent
	END AS _UTMContent
FROM
	int_mart_e2e_funnels_month;
/* на глубину квартал */
-- 1. целевая таблица
CREATE OR REPLACE TABLE int_mart_e2e_touches_quarter
(
	conversionType String,
    conversionDateTime DateTime,
	conversionID String,
	conversionSource String,
	conversionSourceName String,
	_conversionSum Int64,
	touchWeight Float64,
	touchWeights Int64,
	touchNumber Int64,
    phone String,
	email String,
	_UTMMedium String,
    _UTMSource String,
	_UTMCampaign String,
    _UTMTerm String,
	_UTMContent String
)
ENGINE = SummingMergeTree
ORDER BY (conversionType, conversionDateTime, conversionID, conversionSource, conversionSourceName, _conversionSum, touchWeight, touchWeights, phone, email, _UTMMedium, _UTMSource, _UTMCampaign, _UTMTerm, _UTMContent);

-- 2. материализованное представление (триггер на обновление данных целевой таблицы)
DROP VIEW IF EXISTS int_mart_e2e_touches_quarter_mv;
CREATE MATERIALIZED VIEW int_mart_e2e_touches_quarter_mv TO int_mart_e2e_touches_quarter AS
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum AS _conversionSum,
	CASE
		WHEN UTMMedium='direct' THEN 0.000000000001
		WHEN UTMMedium='internal' THEN 0.000000000001
		WHEN UTMMedium='Веб-сайт' THEN 0.000000000001
		ELSE 1.0
	END AS touchWeight,
	(sum(touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)) AS touchWeights,
	(row_number() OVER (PARTITION BY conversionType,conversionID,conversionSource ORDER BY touchDateTime)) AS touchNumber,
    phone,
	email,
	CASE
		WHEN UTMMedium='internal' THEN 'direct'
		WHEN UTMMedium='Веб-сайт' THEN 'direct'
		ELSE UTMMedium
	END AS _UTMMedium,
    UTMSource AS _UTMSource,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMCampaign
	END AS _UTMCampaign,
    CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMTerm
	END AS _UTMTerm,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMContent
	END AS _UTMContent
FROM
	int_mart_e2e_funnels_quarter;

-- 3. загрузка исходных данных
CREATE OR REPLACE VIEW int_mart_e2e_touches_quarter AS
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum AS _conversionSum,
	CASE
		WHEN UTMMedium='direct' THEN 0.000000000001
		WHEN UTMMedium='internal' THEN 0.000000000001
		WHEN UTMMedium='Веб-сайт' THEN 0.000000000001
		ELSE 1.0
	END AS touchWeight,
	(sum(touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)) AS touchWeights,
	(row_number() OVER (PARTITION BY conversionType,conversionID,conversionSource ORDER BY touchDateTime)) AS touchNumber,
    phone,
	email,
	CASE
		WHEN UTMMedium='internal' THEN 'direct'
		WHEN UTMMedium='Веб-сайт' THEN 'direct'
		ELSE UTMMedium
	END AS _UTMMedium,
    UTMSource AS _UTMSource,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMCampaign
	END AS _UTMCampaign,
    CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMTerm
	END AS _UTMTerm,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMContent
	END AS _UTMContent
FROM
	int_mart_e2e_funnels_quarter;
/* на глубину год */
-- 1. целевая таблица
CREATE OR REPLACE TABLE int_mart_e2e_touches_year
(
	conversionType String,
    conversionDateTime DateTime,
	conversionID String,
	conversionSource String,
	conversionSourceName String,
	_conversionSum Int64,
	touchWeight Float64,
	touchWeights Int64,
	touchNumber Int64,
    phone String,
	email String,
	_UTMMedium String,
    _UTMSource String,
	_UTMCampaign String,
    _UTMTerm String,
	_UTMContent String
)
ENGINE = SummingMergeTree
ORDER BY (conversionType, conversionDateTime, conversionID, conversionSource, conversionSourceName, _conversionSum, touchWeight, touchWeights, phone, email, _UTMMedium, _UTMSource, _UTMCampaign, _UTMTerm, _UTMContent);

-- 2. материализованное представление (триггер на обновление данных целевой таблицы)
DROP VIEW IF EXISTS int_mart_e2e_touches_year_mv;
CREATE MATERIALIZED VIEW int_mart_e2e_touches_year_mv TO int_mart_e2e_touches_year AS
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum AS _conversionSum,
	CASE
		WHEN UTMMedium='direct' THEN 0.000000000001
		WHEN UTMMedium='internal' THEN 0.000000000001
		WHEN UTMMedium='Веб-сайт' THEN 0.000000000001
		ELSE 1.0
	END AS touchWeight,
	(sum(touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)) AS touchWeights,
	(row_number() OVER (PARTITION BY conversionType,conversionID,conversionSource ORDER BY touchDateTime)) AS touchNumber,
    phone,
	email,
	CASE
		WHEN UTMMedium='internal' THEN 'direct'
		WHEN UTMMedium='Веб-сайт' THEN 'direct'
		ELSE UTMMedium
	END AS _UTMMedium,
    UTMSource AS _UTMSource,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMCampaign
	END AS _UTMCampaign,
    CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMTerm
	END AS _UTMTerm,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMContent
	END AS _UTMContent
FROM
	int_mart_e2e_funnels_year;

-- 3. загрузка исходных данных
CREATE OR REPLACE VIEW int_mart_e2e_touches_year AS
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum AS _conversionSum,
	CASE
		WHEN UTMMedium='direct' THEN 0.000000000001
		WHEN UTMMedium='internal' THEN 0.000000000001
		WHEN UTMMedium='Веб-сайт' THEN 0.000000000001
		ELSE 1.0
	END AS touchWeight,
	(sum(touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)) AS touchWeights,
	(row_number() OVER (PARTITION BY conversionType,conversionID,conversionSource ORDER BY touchDateTime)) AS touchNumber,
    phone,
	email,
	CASE
		WHEN UTMMedium='internal' THEN 'direct'
		WHEN UTMMedium='Веб-сайт' THEN 'direct'
		ELSE UTMMedium
	END AS _UTMMedium,
    UTMSource AS _UTMSource,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMCampaign
	END AS _UTMCampaign,
    CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMTerm
	END AS _UTMTerm,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMContent
	END AS _UTMContent
FROM
	int_mart_e2e_funnels_year;
/* на всю глубину */
-- 1. целевая таблица
CREATE OR REPLACE TABLE int_mart_e2e_touches_any
(
	conversionType String,
    conversionDateTime DateTime,
	conversionID String,
	conversionSource String,
	conversionSourceName String,
	_conversionSum Int64,
	touchWeight Float64,
	touchWeights Int64,
	touchNumber Int64,
    phone String,
	email String,
	_UTMMedium String,
    _UTMSource String,
	_UTMCampaign String,
    _UTMTerm String,
	_UTMContent String
)
ENGINE = SummingMergeTree
ORDER BY (conversionType, conversionDateTime, conversionID, conversionSource, conversionSourceName, _conversionSum, touchWeight, touchWeights, phone, email, _UTMMedium, _UTMSource, _UTMCampaign, _UTMTerm, _UTMContent);

-- 2. материализованное представление (триггер на обновление данных целевой таблицы)
DROP VIEW IF EXISTS int_mart_e2e_touches_any_mv;
CREATE MATERIALIZED VIEW int_mart_e2e_touches_any_mv TO int_mart_e2e_touches_any AS
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum AS _conversionSum,
	CASE
		WHEN UTMMedium='direct' THEN 0.000000000001
		WHEN UTMMedium='internal' THEN 0.000000000001
		WHEN UTMMedium='Веб-сайт' THEN 0.000000000001
		ELSE 1.0
	END AS touchWeight,
	(sum(touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)) AS touchWeights,
	(row_number() OVER (PARTITION BY conversionType,conversionID,conversionSource ORDER BY touchDateTime)) AS touchNumber,
    phone,
	email,
	CASE
		WHEN UTMMedium='internal' THEN 'direct'
		WHEN UTMMedium='Веб-сайт' THEN 'direct'
		ELSE UTMMedium
	END AS _UTMMedium,
    UTMSource AS _UTMSource,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMCampaign
	END AS _UTMCampaign,
    CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMTerm
	END AS _UTMTerm,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMContent
	END AS _UTMContent
FROM
	int_mart_e2e_funnels_any;

-- 3. загрузка исходных данных
CREATE OR REPLACE VIEW int_mart_e2e_touches_any AS
SELECT 
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum AS _conversionSum,
	CASE
		WHEN UTMMedium='direct' THEN 0.000000000001
		WHEN UTMMedium='internal' THEN 0.000000000001
		WHEN UTMMedium='Веб-сайт' THEN 0.000000000001
		ELSE 1.0
	END AS touchWeight,
	(sum(touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)) AS touchWeights,
	(row_number() OVER (PARTITION BY conversionType,conversionID,conversionSource ORDER BY touchDateTime)) AS touchNumber,
    phone,
	email,
	CASE
		WHEN UTMMedium='internal' THEN 'direct'
		WHEN UTMMedium='Веб-сайт' THEN 'direct'
		ELSE UTMMedium
	END AS _UTMMedium,
    UTMSource AS _UTMSource,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMCampaign
	END AS _UTMCampaign,
    CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMTerm
	END AS _UTMTerm,
	CASE
		WHEN UTMMedium='direct' THEN ''
		WHEN UTMMedium='internal' THEN ''
		WHEN UTMMedium='Веб-сайт' THEN ''
		ELSE UTMContent
	END AS _UTMContent
FROM
	int_mart_e2e_funnels_any;