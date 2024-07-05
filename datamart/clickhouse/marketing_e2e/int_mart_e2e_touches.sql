/* Цепочка касаний из всех источников трафика */
/*
	Дата касания: touchDateTime,
	Тип касания: touchType,
	ID касания (в источнике): touchID,
	Источник касания: touchSource,
	Название в источнике касания: touchSourceName,
	Yandex Client ID: yclid,
	Google Client ID: gclid,
	Google Analytics ID gaid,
	Yandex AppMetrica Installation ID: amiid,
	Calltouch ID: ctid,
	Телефон: phone,
	Email: email,
	Google (Android) ID: google_aid,
	Android Device ID: oaid,
	iOS Ad ID: ios_ifa,
	iOS Device ID: ios_ifv,
	Windows Devide ID: windows_aid,
	Канал касания: UTMMedium,
	Источник касания: UTMSource,
	Кампания касания: UTMCampaign,
	Ключевое слово касания: UTMTerm,
	Содержание касания: UTMContent */
/* необходимо выбрать используемые источники */

CREATE OR REPLACE VIEW int_mart_e2e_touches AS
/* Яндекс.Метрика (визиты) */
SELECT
	'session' AS touchType,
	'yandex.metrika' AS touchSource,
	`ym:s:counterID` AS touchSourceName,
	`ym:s:dateTime` AS touchDateTime,
	`ym:s:visitID` AS touchID,
	`ym:s:clientID` AS yclid,
	`ym:s:lastGCLID` AS gclid,
	'' AS gaid,
	'' AS amiid,
	'' AS ctid,
	'' AS phone,
	'' AS email,
	'' AS google_aid,
	'' AS oaid,
	'' AS ios_ifa,
	'' AS ios_ifv,
	'' AS windows_aid,
	CASE
        WHEN `ym:s:lastUTMMedium`='' THEN `ym:s:lastTrafficSource`
        ELSE IFNULL(`ym:s:lastUTMMedium`, `ym:s:lastTrafficSource`)
    END AS UTMMedium,
    CASE
        WHEN `ym:s:lastUTMMedium`='' OR `ym:s:lastUTMMedium` IS NULL THEN CASE
            WHEN `ym:s:lastTrafficSource`='organic' THEN `ym:s:lastSearchEngineRoot`
            WHEN `ym:s:lastTrafficSource`='referral' THEN `ym:s:lastReferalSource`
            WHEN `ym:s:lastTrafficSource`='ad' THEN `ym:s:lastAdvEngine`
            WHEN `ym:s:lastTrafficSource`='social' THEN `ym:s:lastSocialNetwork`
            WHEN `ym:s:lastTrafficSource`='messenger' THEN `ym:s:lastMessenger`
			WHEN `ym:s:lastTrafficSource`='recommend' THEN `ym:s:lastRecommendationSystem`
            ELSE `ym:s:from` END
        ELSE IFNULL(`ym:s:lastUTMSource`, '')
    END AS UTMSource,
	CASE
        WHEN `ym:s:lastUTMMedium`='' OR `ym:s:lastUTMMedium` IS NULL THEN CASE
            WHEN `ym:s:lastTrafficSource`='organic' THEN `ym:s:lastSearchEngine`
			WHEN `ym:s:lastTrafficSource`='social' THEN `ym:s:lastSocialNetworkProfile`
			WHEN `ym:s:lastTrafficSource`='ad' THEN `ym:s:lastDirectClickOrder`
			ELSE IFNULL(`ym:s:lastUTMCampaign`, '') END
		ELSE IFNULL(`ym:s:lastUTMCampaign`, '')
    END AS UTMCampaign,
    `ym:s:lastUTMTerm` AS UTMTerm,
	`ym:s:lastUTMContent` AS UTMContent
FROM
	raw_ym_visits

/* Яндекс.Метрика (цели) */
UNION ALL
SELECT
	'goal' AS touchType,
	'yandex.metrika' AS touchSource,
	'' AS touchSourceName,
	`ym:s:goalDateTime` AS touchDateTime,
	`ym:s:goalID` AS touchID,
	`ym:s:clientID` AS yclid,
	'' AS gclid,
	'' AS gaid,
	'' AS amiid,
	'' AS ctid,
	'' AS phone,
	'' AS email,
	'' AS google_aid,
	'' AS oaid,
	'' AS ios_ifa,
	'' AS ios_ifv, 
	'' AS windows_aid,
	'' AS UTMMedium,
    '' AS UTMSource,
	'' AS UTMCampaign,
    '' AS UTMTerm,
	'' AS UTMContent
FROM
	raw_ym_visits_goals

/* Яндекс.Аппметрика (установки) */
/*
UNION ALL
SELECT
	CASE
		WHEN `is_reinstallation`=1 THEN 'reinstallation'
		ELSE 'installation'
	END AS touchType,
	'yandex.appmetrica' AS touchSource,
	`app_package_name` AS touchSourceName,
	`install_datetime` AS touchDateTime,
	`installation_id` AS touchID,
	'' AS yclid,
	'' AS gclid,
	'' AS gaid,
	'' AS amiid,
	'' AS ctid,
	'' AS phone,
	'' AS email,
	`google_aid` AS google_aid,
	`oaid` AS oaid,
	`ios_ifa` AS ios_ifa,
	`ios_ifv` AS ios_ifv, 
	`windows_aid` AS windows_aid,
	CASE
		WHEN position(`click_url_parameters`, 'utm_medium=') > 0 THEN decodeURLComponent(replace(extract(`click_url_parameters`, 'utm_medium=([^&]+)'), '%28not%20set%29', ''))
		WHEN `tracker_name`='Google Play' THEN 'organic'
		WHEN `publisher_name`='Google Search' THEN 'organic'
		WHEN `publisher_name`='Яндекс карты' THEN 'organic'
		WHEN `publisher_name`='2gis' THEN 'organic'
		WHEN `publisher_name`='instagram' THEN 'smm'
		WHEN `publisher_name`='Sun' THEN 'smm'
		WHEN `publisher_name`='Yandex.Direct' THEN 'cpc'
		WHEN `publisher_name`='' AND `tracker_name`<>'unknown' THEN 'other'
		WHEN `publisher_name`<>'' THEN 'other'
		ELSE 'direct'
	END AS UTMMedium,
    CASE
		WHEN position(`click_url_parameters`, 'utm_source=') > 0 THEN decodeURLComponent(replace(extract(`click_url_parameters`, 'utm_source=([^&]+)'), '%28not%20set%29', ''))
		WHEN `publisher_name`='unknown' THEN ''
		ELSE `publisher_name`
	END AS UTMSource,
	CASE
		WHEN `tracker_name`='unknown' THEN ''
		ELSE `tracker_name`
	END AS UTMCampaign,
    '' AS UTMTerm,
	'' AS UTMContent
FROM
	raw_ya_installs
*/
/* Яндекс.Аппметрика (события) */
/*
UNION ALL
SELECT
	CONCAT('ya_', `event_name`) AS touchType,
	'yandex.appmetrica' AS touchSource,
	`app_package_name` AS touchSourceName,
	`event_datetime` AS touchDateTime,
	toString(`session_id`) AS touchID,
	'' AS yclid,
	'' AS gclid,
	'' AS gaid,
	`installation_id` AS amiid,
	'' AS ctid,
	'' AS phone,
	'' AS email,
	`google_aid` AS google_aid,
	'' AS oaid,
	`ios_ifa` AS ios_ifa,
	`ios_ifv` AS ios_ifv,
	`windows_aid` AS windows_aid,
	'' AS UTMMedium,
    '' AS UTMSource,
	'' AS UTMCampaign,
    '' AS UTMTerm,
	'' AS UTMContent
FROM
	raw_ya_events
*/
/* Calltouch (звонки) */
/*
UNION ALL
SELECT
	'call' AS touchType,
	'calltouch' AS touchSource,
	`hostname` AS touchSourceName,
	`date` AS touchDateTime,
	`sipCallId` AS touchID,
	`yaClientId` AS yclid,
	'' as gclid,
	`clientId` AS gaid,
	'' AS amiid,
	toString(`ctClientId`) AS ctid,
	toString(`callerNumber`) AS phone,
	'' AS email,
	'' AS google_aid,
	'' AS oaid,
	'' AS ios_ifa,
	'' AS ios_ifv, 
	'' AS windows_aid,
	CASE
		WHEN `utmMedium`='<не указано>' THEN 'direct'
		ELSE `utmMedium`
	END AS UTMMedium,
    CASE
		WHEN `utmSource`='<не указано>' THEN ''
		ELSE `utmSource`
	END AS UTMSource,
	CASE
		WHEN `utmCampaign`='<не указано>' THEN ''
		ELSE `utmCampaign`
	END AS UTMCampaign,
    CASE
		WHEN `utmTerm`='<не указано>' THEN ''
		WHEN `utmTerm`='<не заполнено>' THEN ''
		ELSE `utmTerm`
	END AS UTMTerm,
	CASE
		WHEN `utmContent`='<не указано>' THEN ''
		WHEN `utmContent`='<не заполнено>' THEN ''
		ELSE `utmContent`
	END AS UTMContent
FROM
	raw_ct_calls
*/
/* Битрикс24 (лиды) */
/*
UNION ALL
SELECT
	'lead' AS touchType,
	'bitrix24' AS touchSource,
	'' AS touchSourceName,
	`DATE_CREATE` AS touchDateTime,
	toString(`ID`) AS touchID,
	'' AS yclid,
	'' as gclid,
	'' AS gaid,
	'' AS amiid,
	'' AS ctid,
	CASE
		WHEN LENGTH(`phone1`)<11 THEN ''
		ELSE CONCAT('7', SUBSTRING(replace(replace(replace(replace(replace(`phone1`, '(', ''), ')', ''), ' ', ''), '+', ''), '-', ''), 2))
	END AS phone,
	`email` AS email,
	'' AS google_aid,
	'' AS oaid,
	'' AS ios_ifa,
	'' AS ios_ifv, 
	'' AS windows_aid,
    CASE
        WHEN `UTM_SOURCE`='(direct)' THEN 'bitrix24'
        WHEN `UTM_MEDIUM`='' THEN IFNULL(s.NAME, 'direct')
        WHEN `UTM_MEDIUM` IS NULL THEN IFNULL(s.NAME, 'direct')
        WHEN `UTM_MEDIUM`='(none)' THEN IFNULL(s.NAME, 'direct')
        ELSE IFNULL(`UTM_MEDIUM`, IFNULL(s.NAME, 'direct'))
    END as UTMMedium,
    CASE 
        WHEN `UTM_SOURCE`='(offline)' THEN IFNULL(`UTM_MEDIUM`, '')
        WHEN `UTM_SOURCE`='' THEN IFNULL(s.NAME, '')
        WHEN `UTM_SOURCE`='(none)' THEN IFNULL(s.NAME, '')
        WHEN `UTM_SOURCE`='(direct)' THEN IFNULL(s.NAME, '')
        WHEN `UTM_SOURCE` IS NULL THEN IFNULL(s.NAME, '')
        ELSE IFNULL(`UTM_SOURCE`, '')
    END as UTMSource,
	CASE 
        WHEN `UTM_CAMPAIGN`='(referral)' THEN ''
        WHEN `UTM_CAMPAIGN`='(organic)' THEN ''
        WHEN `UTM_CAMPAIGN`='(none)' THEN ''
        WHEN `UTM_CAMPAIGN`='(undefined)' THEN ''
        ELSE IFNULL(`UTM_CAMPAIGN`, '')
    END as UTMCampaign,
	CASE
		WHEN `UTM_TERM`='<не указано>' THEN ''
		WHEN `UTM_TERM`='<не заполнено>' THEN ''
		ELSE `UTM_TERM`
	END AS UTMTerm,
	CASE
		WHEN `UTM_CONTENT`='<не указано>' THEN ''
		WHEN `UTM_CONTENT`='<не заполнено>' THEN ''
		ELSE `UTM_CONTENT`
	END AS UTMContent
FROM
	raw_bx_crm_lead_uf as l
LEFT ANY JOIN raw_bx_crm_status as s ON l.SOURCE_ID=s.STATUS_ID
*/
/* Битрикс24 (сделки) */
/*
UNION ALL
SELECT
	'deal' AS touchType,
	'bitrix24' AS touchSource,
	'' AS touchSourceName,
	`DATE_CREATE` AS touchDateTime,
	toString(`ID`) AS touchID,
	'' AS yclid,
	'' as gclid,
	'' AS gaid,
	'' AS amiid,
	'' AS ctid,
	CASE
		WHEN LENGTH(`phone1`)<11 THEN ''
		ELSE CONCAT('7', SUBSTRING(replace(replace(replace(replace(replace(`phone1`, '(', ''), ')', ''), ' ', ''), '+', ''), '-', ''), 2))
	END AS phone,
	`email` AS email,
	'' AS google_aid,
	'' AS oaid,
	'' AS ios_ifa,
	'' AS ios_ifv, 
	'' AS windows_aid,
    CASE
        WHEN `UTM_SOURCE`='(direct)' THEN 'bitrix24'
        WHEN `UTM_MEDIUM`='' THEN IFNULL(s.NAME, 'direct')
        WHEN `UTM_MEDIUM` IS NULL THEN IFNULL(s.NAME, 'direct')
        WHEN `UTM_MEDIUM`='(none)' THEN IFNULL(s.NAME, 'direct')
        ELSE IFNULL(`UTM_MEDIUM`, IFNULL(s.NAME, 'direct'))
    END as UTMMedium,
    CASE 
        WHEN `UTM_SOURCE`='(offline)' THEN IFNULL(`UTM_MEDIUM`, '')
        WHEN `UTM_SOURCE`='' THEN IFNULL(s.NAME, '')
        WHEN `UTM_SOURCE`='(none)' THEN IFNULL(s.NAME, '')
        WHEN `UTM_SOURCE`='(direct)' THEN IFNULL(s.NAME, '')
        WHEN `UTM_SOURCE` IS NULL THEN IFNULL(s.NAME, '')
        ELSE IFNULL(`UTM_SOURCE`, '')
    END as UTMSource,
	CASE 
        WHEN `UTM_CAMPAIGN`='(referral)' THEN ''
        WHEN `UTM_CAMPAIGN`='(organic)' THEN ''
        WHEN `UTM_CAMPAIGN`='(none)' THEN ''
        WHEN `UTM_CAMPAIGN`='(undefined)' THEN ''
        ELSE IFNULL(`UTM_CAMPAIGN`, '')
    END as UTMCampaign,
	CASE
		WHEN `UTM_TERM`='<не указано>' THEN ''
		WHEN `UTM_TERM`='<не заполнено>' THEN ''
		ELSE `UTM_TERM`
	END AS UTMTerm,
	CASE
		WHEN `UTM_CONTENT`='<не указано>' THEN ''
		WHEN `UTM_CONTENT`='<не заполнено>' THEN ''
		ELSE `UTM_CONTENT`
	END AS UTMContent
FROM
	raw_bx_crm_deal_uf as d
LEFT ANY JOIN raw_bx_crm_status as s ON d.SOURCE_ID=s.STATUS_ID
*/