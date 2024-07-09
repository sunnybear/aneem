/* Конверсии с атрибуцией по модели полураспада (Time Deecay) (ценность экспоненциально падает с течением времени) */
/*
	final_mart_e2e_td_3days
	final_mart_e2e_td_week
	final_mart_e2e_td_month
	final_mart_e2e_td_quarter
	final_mart_e2e_td_year
	final_mart_e2e_td_any
*/
/*
	Тип конверсии: conversionType,
	Дата конверсии: conversionDateTime,
	ID конверсии (в источнике): conversionID,
	Источник конверсии: conversionSource,
	Название в источнике конверсии: conversionSourceName,
	Вес конверсии: conversionWeight,
	Сумма конверсии: conversionSum,
	Телефон: phone,
	Email: email,
	Канал TD: UTMMedium,
	Источник TD: UTMSource,
	Кампания TD: UTMCampaign,
	Ключевое слово TD: UTMTerm,
	Содержание TD: UTMContent */
/* на глубину 3 дня */
CREATE OR REPLACE VIEW final_mart_e2e_td_3days AS
SELECT
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	round(pow(2, -touchIndex)*touchWeight/touchWeightsSum, 4)+0.0 AS conversionWeight,
	round(_conversionSum*pow(2, -touchIndex)*touchWeight/touchWeightsSum, 4)+0.0 AS conversionSum,
    phone,
	email,
    UTMMedium,
    UTMSource,
	UTMCampaign,
    UTMTerm,
	UTMContent
FROM (SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	(sum(pow(2, -touchIndex)*touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)) AS touchWeightsSum,
	touchIndex,
	touchWeight,
	_conversionSum,
    phone,
	email,
    _UTMMedium AS UTMMedium,
    _UTMSource AS UTMSource,
	_UTMCampaign AS UTMCampaign,
    _UTMTerm AS UTMTerm,
	_UTMContent AS UTMContent
FROM int_mart_e2e_touches_3days);
/* на глубину неделя */
CREATE OR REPLACE VIEW final_mart_e2e_td_week AS
SELECT
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	round(pow(2, -touchIndex)*touchWeight/touchWeightsSum, 4)+0.0 AS conversionWeight,
	round(_conversionSum*pow(2, -touchIndex)*touchWeight/touchWeightsSum, 4)+0.0 AS conversionSum,
    phone,
	email,
    UTMMedium,
    UTMSource,
	UTMCampaign,
    UTMTerm,
	UTMContent
FROM (SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	(sum(pow(2, -touchIndex)*touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)) AS touchWeightsSum,
	touchIndex,
	touchWeight,
	_conversionSum,
    phone,
	email,
    _UTMMedium AS UTMMedium,
    _UTMSource AS UTMSource,
	_UTMCampaign AS UTMCampaign,
    _UTMTerm AS UTMTerm,
	_UTMContent AS UTMContent
FROM int_mart_e2e_touches_week);
/* на глубину месяц */
CREATE OR REPLACE VIEW final_mart_e2e_td_month AS
SELECT
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	round(pow(2, -touchIndex)*touchWeight/touchWeightsSum, 4)+0.0 AS conversionWeight,
	round(_conversionSum*pow(2, -touchIndex)*touchWeight/touchWeightsSum, 4)+0.0 AS conversionSum,
    phone,
	email,
    UTMMedium,
    UTMSource,
	UTMCampaign,
    UTMTerm,
	UTMContent
FROM (SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	(sum(pow(2, -touchIndex)*touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)) AS touchWeightsSum,
	touchIndex,
	touchWeight,
	_conversionSum,
    phone,
	email,
    _UTMMedium AS UTMMedium,
    _UTMSource AS UTMSource,
	_UTMCampaign AS UTMCampaign,
    _UTMTerm AS UTMTerm,
	_UTMContent AS UTMContent
FROM int_mart_e2e_touches_month);
/* на глубину квартал */
CREATE OR REPLACE VIEW final_mart_e2e_td_quarter AS
SELECT
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	round(pow(2, -touchIndex)*touchWeight/touchWeightsSum, 4)+0.0 AS conversionWeight,
	round(_conversionSum*pow(2, -touchIndex)*touchWeight/touchWeightsSum, 4)+0.0 AS conversionSum,
    phone,
	email,
    UTMMedium,
    UTMSource,
	UTMCampaign,
    UTMTerm,
	UTMContent
FROM (SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	(sum(pow(2, -touchIndex)*touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)) AS touchWeightsSum,
	touchIndex,
	touchWeight,
	_conversionSum,
    phone,
	email,
    _UTMMedium AS UTMMedium,
    _UTMSource AS UTMSource,
	_UTMCampaign AS UTMCampaign,
    _UTMTerm AS UTMTerm,
	_UTMContent AS UTMContent
FROM int_mart_e2e_touches_quarter);
/* на глубину год */
CREATE OR REPLACE VIEW final_mart_e2e_td_year AS
SELECT
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	round(pow(2, -touchIndex)*touchWeight/touchWeightsSum, 4)+0.0 AS conversionWeight,
	round(_conversionSum*pow(2, -touchIndex)*touchWeight/touchWeightsSum, 4)+0.0 AS conversionSum,
    phone,
	email,
    UTMMedium,
    UTMSource,
	UTMCampaign,
    UTMTerm,
	UTMContent
FROM (SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	(sum(pow(2, -touchIndex)*touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)) AS touchWeightsSum,
	touchIndex,
	touchWeight,
	_conversionSum,
    phone,
	email,
    _UTMMedium AS UTMMedium,
    _UTMSource AS UTMSource,
	_UTMCampaign AS UTMCampaign,
    _UTMTerm AS UTMTerm,
	_UTMContent AS UTMContent
FROM int_mart_e2e_touches_year);
/* на всю глубину */
CREATE OR REPLACE VIEW final_mart_e2e_td_any AS
SELECT
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	round(pow(2, -touchIndex)*touchWeight/touchWeightsSum, 4)+0.0 AS conversionWeight,
	round(_conversionSum*pow(2, -touchIndex)*touchWeight/touchWeightsSum, 4)+0.0 AS conversionSum,
    phone,
	email,
    UTMMedium,
    UTMSource,
	UTMCampaign,
    UTMTerm,
	UTMContent
FROM (SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	(sum(pow(2, -touchIndex)*touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)) AS touchWeightsSum,
	touchIndex,
	touchWeight,
	_conversionSum,
    phone,
	email,
    _UTMMedium AS UTMMedium,
    _UTMSource AS UTMSource,
	_UTMCampaign AS UTMCampaign,
    _UTMTerm AS UTMTerm,
	_UTMContent AS UTMContent
FROM int_mart_e2e_touches_any);