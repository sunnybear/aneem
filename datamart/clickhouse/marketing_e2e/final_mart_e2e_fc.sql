/* Конверсии с атрибуцией по FC (первое касание) */
/*
	final_mart_e2e_fc_3days
	final_mart_e2e_fc_week
	final_mart_e2e_fc_month
	final_mart_e2e_fc_quarter
	final_mart_e2e_fc_year
	final_mart_e2e_fc_any
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
	Канал FC: UTMMedium,
	Источник FC: UTMSource,
	Кампания FC: UTMCampaign,
	Ключевое слово FC: UTMTerm,
	Содержание FC: UTMContent */
/* на глубину 3 дня */
CREATE OR REPLACE VIEW final_mart_e2e_fc_3days AS
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
    argMin(UTMMedium, (touchDateTime)) AS UTMMedium,
    argMin(UTMSource, (touchDateTime)) AS UTMSource,
	argMin(UTMCampaign, (touchDateTime)) AS UTMCampaign,
    argMin(UTMTerm, (touchDateTime)) AS UTMTerm,
	argMin(UTMContent, (touchDateTime)) AS UTMContent
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
/* на глубину неделя */
CREATE OR REPLACE VIEW final_mart_e2e_fc_week AS
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
    argMin(UTMMedium, (touchDateTime)) AS UTMMedium,
    argMin(UTMSource, (touchDateTime)) AS UTMSource,
	argMin(UTMCampaign, (touchDateTime)) AS UTMCampaign,
    argMin(UTMTerm, (touchDateTime)) AS UTMTerm,
	argMin(UTMContent, (touchDateTime)) AS UTMContent
FROM
	int_mart_e2e_funnels_week
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
CREATE OR REPLACE VIEW final_mart_e2e_fc_month AS
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
    argMin(UTMMedium, (touchDateTime)) AS UTMMedium,
    argMin(UTMSource, (touchDateTime)) AS UTMSource,
	argMin(UTMCampaign, (touchDateTime)) AS UTMCampaign,
    argMin(UTMTerm, (touchDateTime)) AS UTMTerm,
	argMin(UTMContent, (touchDateTime)) AS UTMContent
FROM
	int_mart_e2e_funnels_month
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
CREATE OR REPLACE VIEW final_mart_e2e_fc_quarter AS
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
    argMin(UTMMedium, (touchDateTime)) AS UTMMedium,
    argMin(UTMSource, (touchDateTime)) AS UTMSource,
	argMin(UTMCampaign, (touchDateTime)) AS UTMCampaign,
    argMin(UTMTerm, (touchDateTime)) AS UTMTerm,
	argMin(UTMContent, (touchDateTime)) AS UTMContent
FROM
	int_mart_e2e_funnels_quarter
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
CREATE OR REPLACE VIEW final_mart_e2e_fc_year AS
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
    argMin(UTMMedium, (touchDateTime)) AS UTMMedium,
    argMin(UTMSource, (touchDateTime)) AS UTMSource,
	argMin(UTMCampaign, (touchDateTime)) AS UTMCampaign,
    argMin(UTMTerm, (touchDateTime)) AS UTMTerm,
	argMin(UTMContent, (touchDateTime)) AS UTMContent
FROM
	int_mart_e2e_funnels_year
GROUP BY
	conversionType,
    conversionDateTime,
	conversionID,
	conversionSource,
	conversionSourceName,
	conversionSum,
    phone,
	email;