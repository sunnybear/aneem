/* Конверсии с атрибуцией по модели U-shape (40%-20%-40%) (ценность на первое и последнее касание, остальное - равномерно между ними) */
/*
	final_mart_e2e_ushape_3days
	final_mart_e2e_ushape_week
	final_mart_e2e_ushape_month
	final_mart_e2e_ushape_quarter
	final_mart_e2e_ushape_year
	final_mart_e2e_ushape_any
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
	Канал Ushape: UTMMedium,
	Источник Ushape: UTMSource,
	Кампания Ushape: UTMCampaign,
	Ключевое слово Ushape: UTMTerm,
	Содержание Ushape: UTMContent */
/* на глубину 3 дня */
CREATE OR REPLACE VIEW final_mart_e2e_ushape_3days AS
SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	round(IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight/(sum(IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)), 4)+0.0 AS conversionWeight,
	round(_conversionSum * IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight/(sum(IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)), 4)+0.0 AS conversionSum,
    phone,
	email,
    _UTMMedium AS UTMMedium,
    _UTMSource AS UTMSource,
	_UTMCampaign AS UTMCampaign,
    _UTMTerm AS UTMTerm,
	_UTMContent AS UTMContent
FROM int_mart_e2e_touches_3days;
/* на глубину неделя */
CREATE OR REPLACE VIEW final_mart_e2e_ushape_week AS
SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	round(IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight/(sum(IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)), 4)+0.0 AS conversionWeight,
	round(_conversionSum * IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight/(sum(IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)), 4)+0.0 AS conversionSum,
    phone,
	email,
    _UTMMedium AS UTMMedium,
    _UTMSource AS UTMSource,
	_UTMCampaign AS UTMCampaign,
    _UTMTerm AS UTMTerm,
	_UTMContent AS UTMContent
FROM int_mart_e2e_touches_week;
/* на глубину месяц */
CREATE OR REPLACE VIEW final_mart_e2e_ushape_month AS
SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	round(IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight/(sum(IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)), 4)+0.0 AS conversionWeight,
	round(_conversionSum * IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight/(sum(IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)), 4)+0.0 AS conversionSum,
    phone,
	email,
    _UTMMedium AS UTMMedium,
    _UTMSource AS UTMSource,
	_UTMCampaign AS UTMCampaign,
    _UTMTerm AS UTMTerm,
	_UTMContent AS UTMContent
FROM int_mart_e2e_touches_month;
/* на глубину квартал */
CREATE OR REPLACE VIEW final_mart_e2e_ushape_quarter AS
SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	round(IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight/(sum(IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)), 4)+0.0 AS conversionWeight,
	round(_conversionSum * IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight/(sum(IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)), 4)+0.0 AS conversionSum,
    phone,
	email,
    _UTMMedium AS UTMMedium,
    _UTMSource AS UTMSource,
	_UTMCampaign AS UTMCampaign,
    _UTMTerm AS UTMTerm,
	_UTMContent AS UTMContent
FROM int_mart_e2e_touches_quarter;
/* на глубину год */
CREATE OR REPLACE VIEW final_mart_e2e_ushape_year AS
SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	round(IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight/(sum(IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)), 4)+0.0 AS conversionWeight,
	round(_conversionSum * IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight/(sum(IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)), 4)+0.0 AS conversionSum,
    phone,
	email,
    _UTMMedium AS UTMMedium,
    _UTMSource AS UTMSource,
	_UTMCampaign AS UTMCampaign,
    _UTMTerm AS UTMTerm,
	_UTMContent AS UTMContent
FROM int_mart_e2e_touches_year;
/* на всю глубину */
CREATE OR REPLACE VIEW final_mart_e2e_ushape_any AS
SELECT 
	conversionType,
	conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	round(IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight/(sum(IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)), 4)+0.0 AS conversionWeight,
	round(_conversionSum * IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight/(sum(IF(touchCount=1,1,IF(touchCount=2,0.5,IF(touchIndex=1 OR touchIndex=touchCount,0.4,0.2/(touchCount-2))))*touchWeight) OVER (PARTITION BY conversionType,conversionID,conversionSource)), 4)+0.0 AS conversionSum,
    phone,
	email,
    _UTMMedium AS UTMMedium,
    _UTMSource AS UTMSource,
	_UTMCampaign AS UTMCampaign,
    _UTMTerm AS UTMTerm,
	_UTMContent AS UTMContent
FROM int_mart_e2e_touches_any;