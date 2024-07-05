/* Конверсии с атрибуцией по LC (последнее касание) */
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
	Канал LC: UTMMedium,
	Источник LC: UTMSource,
	Кампания LC: UTMCampaign,
	Ключевое слово LC: UTMTerm,
	Содержание LC: UTMContent */

CREATE OR REPLACE VIEW final_mart_e2e_lc AS
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
    argMax(UTMMedium, (touchDateTime)) as UTMMedium,
    argMax(UTMSource, (touchDateTime)) as UTMSource,
	argMax(UTMCampaign, (touchDateTime)) as UTMCampaign,
    argMax(UTMTerm, (touchDateTime)) as UTMTerm,
	argMax(UTMContent, (touchDateTime)) as UTMContent
FROM
	int_mart_e2e_funnels_3days
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email;