-- 1. ground table
CREATE TABLE DB.dict_yainstallationid_yclid
(
    `install_datetime` DateTime,
    `installation_id` String,
    `yclid` String,
)
ENGINE = SummingMergeTree
ORDER BY (install_datetime, installation_id, yclid);

-- 2. materialized view (updates data rom now)
CREATE MATERIALIZED VIEW DB.dict_yainstallationid_yclid_mv TO DB.dict_yainstallationid_yclid AS
SELECT
	install_datetime,
	installation_id,
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
FROM DB.raw_am_installs as i
    LEFT JOIN DB.raw_ym_visits as v ON iyear=vyear
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

-- 3. initial data upload
INSERT INTO DB.dict_yainstallationid_yclid SELECT
    install_datetime,
    installation_id,
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
FROM DB.raw_am_installs as i
    LEFT JOIN DB.raw_ym_visits as v ON iyear=vyear
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