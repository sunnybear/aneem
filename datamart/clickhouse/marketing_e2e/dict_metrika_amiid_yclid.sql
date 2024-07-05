/* Словарь соответствия yclid-amiid для Yandex.Appmetrica (из соответствия устройства и захода по Яндекс.Метрике) */
/*
	Yandex Appmetrica Installation ID: amiid,
	Yandex Client ID: yclid */
-- 1. целевая таблица
CREATE OR REPLACE TABLE dict_metrika_amiid_yclid
(
    `amiid` String,
    `yclid` String,
)
ENGINE = SummingMergeTree
ORDER BY (amiid, yclid);

-- 2. материализованное представление (триггер на обновление данных целевой таблицы)
DROP VIEW IF EXISTS dict_metrika_amiid_yclid_mv;
CREATE MATERIALIZED VIEW dict_metrika_amiid_yclid_mv TO dict_metrika_amiid_yclid AS
SELECT
	installation_id AS amiid,
	yclid
FROM (SELECT
	installation_id,
	install_datetime,
	install_datetime-`ym:s:dateTime` as df,
	ROW_NUMBER() OVER (PARTITION BY install_datetime ORDER BY df) AS rowNum,
	YEAR(install_datetime) as iyear,
	MONTH(install_datetime) as imonth,
	DAYOFMONTH(install_datetime) as iday,
	YEAR(`ym:s:dateTime`) as vyear,
	MONTH(`ym:s:dateTime`) as vmonth,
	DAYOFMONTH(`ym:s:dateTime`) as vday,
	lower(device_manufacturer) as imanufacturer,
	CASE
		WHEN lower(os_name)='ios' THEN concat(lower(os_name), SUBSTRING(os_version, 1, 2))
		ELSE concat(lower(os_name), '_',os_version)
	END as ios,
	SUBSTRING(device_locale, 1, 2) as ilocale,
	`ym:s:mobilePhone` as vmanufacturer,
	`ym:s:operatingSystem` as vos,
	`ym:s:browserLanguage` as vlocale,
	`ym:s:clientID` as yclid
FROM raw_ya_installs as i
    LEFT JOIN raw_ym_visits as v ON iyear=vyear
        AND imonth=vmonth
        AND iday=vday
        AND imanufacturer=vmanufacturer
        AND ilocale=vlocale
        AND ios=vos
WHERE `ym:s:visitDuration`>5
AND (publisher_name='<WEBSITE>' or publisher_name='<Web2App>' or publisher_name='')
AND df>0
AND df<600)
WHERE rowNum=1;

-- 3. загрузка исходных данных
INSERT INTO dict_metrika_amiid_yclid SELECT
    installation_id as amiid,
    yclid
FROM (SELECT
    installation_id,
    install_datetime,
    install_datetime-`ym:s:dateTime` as df,
    ROW_NUMBER() OVER (PARTITION BY install_datetime ORDER BY df) AS rowNum,
    YEAR(install_datetime) as iyear,
    MONTH(install_datetime) as imonth,
    DAYOFMONTH(install_datetime) as iday,
    YEAR(`ym:s:dateTime`) as vyear,
    MONTH(`ym:s:dateTime`) as vmonth,
    DAYOFMONTH(`ym:s:dateTime`) as vday,
    lower(device_manufacturer) as imanufacturer,
    CASE
        WHEN lower(os_name)='ios' THEN concat(lower(os_name), SUBSTRING(os_version, 1, 2))
        ELSE concat(lower(os_name), '_',os_version)
    END as ios,
    SUBSTRING(device_locale, 1, 2) as ilocale,
    `ym:s:mobilePhone` as vmanufacturer,
    `ym:s:operatingSystem` as vos,
    `ym:s:browserLanguage` as vlocale,
    `ym:s:clientID` as yclid
FROM raw_ya_installs as i
    LEFT JOIN raw_ym_visits as v ON iyear=vyear
        AND imonth=vmonth
        AND iday=vday
        AND imanufacturer=vmanufacturer
        AND ilocale=vlocale
        AND ios=vos
WHERE `ym:s:visitDuration`>5
AND (publisher_name='<WEBSITE>' or publisher_name='<Web2App>' or publisher_name='')
AND df>0
AND df<600)
WHERE rowNum=1;