/* Конверсии с атрибуцией по линейной модели (ценность распределена равномерно по всем касаниям) */
/*
	final_mart_e2e_lin_3days
	final_mart_e2e_lin_week
	final_mart_e2e_lin_month
	final_mart_e2e_lin_quarter
	final_mart_e2e_lin_year
	final_mart_e2e_lin_any
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
	Канал LIN: UTMMedium,
	Источник LIN: UTMSource,
	Кампания LIN: UTMCampaign,
	Ключевое слово LIN: UTMTerm,
	Содержание LIN: UTMContent */
/* на глубину 3 дня */
CREATE OR REPLACE VIEW final_mart_e2e_lin_3days AS
SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	round(touchWeight / touchWeights, 4)+0.0 AS conversionWeight,
	round(_conversionSum * touchWeight / touchWeights, 4)+0.0 AS conversionSum,
    phone,
	email,
    _UTMMedium AS UTMMedium,
    _UTMSource AS UTMSource,
	_UTMCampaign AS UTMCampaign,
    _UTMTerm AS UTMTerm,
	_UTMContent AS UTMContent
FROM int_mart_e2e_touches_3days;
/* на глубину неделя */
CREATE OR REPLACE VIEW final_mart_e2e_lin_week AS
SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	round(touchWeight / touchWeights, 4)+0.0 AS conversionWeight,
	round(_conversionSum * touchWeight / touchWeights, 4)+0.0 AS conversionSum,
    phone,
	email,
    _UTMMedium AS UTMMedium,
    _UTMSource AS UTMSource,
	_UTMCampaign AS UTMCampaign,
    _UTMTerm AS UTMTerm,
	_UTMContent AS UTMContent
FROM int_mart_e2e_touches_week;
/* на глубину месяц */
CREATE OR REPLACE VIEW final_mart_e2e_lin_month AS
SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	round(touchWeight / touchWeights, 4)+0.0 AS conversionWeight,
	round(_conversionSum * touchWeight / touchWeights, 4)+0.0 AS conversionSum,
    phone,
	email,
    _UTMMedium AS UTMMedium,
    _UTMSource AS UTMSource,
	_UTMCampaign AS UTMCampaign,
    _UTMTerm AS UTMTerm,
	_UTMContent AS UTMContent
FROM int_mart_e2e_touches_month;
/* на глубину квартал */
CREATE OR REPLACE VIEW final_mart_e2e_lin_quarter AS
SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	round(touchWeight / touchWeights, 4)+0.0 AS conversionWeight,
	round(_conversionSum * touchWeight / touchWeights, 4)+0.0 AS conversionSum,
    phone,
	email,
    _UTMMedium AS UTMMedium,
    _UTMSource AS UTMSource,
	_UTMCampaign AS UTMCampaign,
    _UTMTerm AS UTMTerm,
	_UTMContent AS UTMContent
FROM int_mart_e2e_touches_quarter;
/* на глубину год */
CREATE OR REPLACE VIEW final_mart_e2e_lin_year AS
SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	round(touchWeight / touchWeights, 4)+0.0 AS conversionWeight,
	round(_conversionSum * touchWeight / touchWeights, 4)+0.0 AS conversionSum,
    phone,
	email,
    _UTMMedium AS UTMMedium,
    _UTMSource AS UTMSource,
	_UTMCampaign AS UTMCampaign,
    _UTMTerm AS UTMTerm,
	_UTMContent AS UTMContent
FROM int_mart_e2e_touches_year;
/* на всю глубину */
CREATE OR REPLACE VIEW final_mart_e2e_lin_any AS
SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	round(touchWeight / touchWeights, 4)+0.0 AS conversionWeight,
	round(_conversionSum * touchWeight / touchWeights, 4)+0.0 AS conversionSum,
    phone,
	email,
    _UTMMedium AS UTMMedium,
    _UTMSource AS UTMSource,
	_UTMCampaign AS UTMCampaign,
    _UTMTerm AS UTMTerm,
	_UTMContent AS UTMContent
FROM int_mart_e2e_touches_any;