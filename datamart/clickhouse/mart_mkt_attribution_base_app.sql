CREATE VIEW DB.mart_mkt_attribution_base_app AS 

SELECT
    `Date` as `_Дата`,
    IFNULL(SUM(`Visits`), 0) AS `_Визиты`,
    IFNULL(SUM(`Costs`), 0.0) AS `_Расходы`,
	IFNULL(SUM(`Installs`), 0.0) AS `_Установки`,
    IFNULL(SUM(`Leads`), 0) AS `_Лиды`,
    IFNULL(SUM(`Deals`), 0) AS `_Сделки`,
    IFNULL(SUM(`Revenue`), 0.0) AS `_Выручка`,
    IFNULL(SUM(`RepeatDeals`), 0) AS `_ПовторныеСделки`,
    CASE 
    WHEN `Channel`='app' THEN 'Мобильное приложение'
    WHEN `Channel`='sms' THEN 'СМС'
    WHEN `Channel`='mail' THEN 'Электронная почта'
    WHEN `Channel`='email' THEN 'Электронная почта'
    WHEN `Channel`='3Demail' THEN 'Электронная почта'
    WHEN `Source`='e.mail.ru' THEN 'Электронная почта'
    WHEN `Source`='click.mail.ru' THEN 'Электронная почта'
    WHEN `Channel`='smm' THEN 'Социальные сети'
    WHEN `Channel`='Social' THEN 'Социальные сети'
	WHEN `Channel`='social' THEN 'Социальные сети'
	WHEN `Channel`='social/' THEN 'Социальные сети'
    WHEN `Source`='Social network traffic' THEN 'Социальные сети'
    WHEN `Source`='m.vk.com' THEN 'Социальные сети'
    WHEN `Source`='away.vk.com' THEN 'Социальные сети'
	WHEN `Channel`='paidsocial' THEN 'Социальные сети'
    WHEN `Channel`='messenger' THEN 'Мессенджеры'
	WHEN `Source`='whatsapp' THEN 'Мессенджеры'
    WHEN `Source`='Messenger traffic' THEN 'Мессенджеры'
    WHEN `Channel`='Chat' THEN 'Мессенджеры'
    WHEN `Channel`='referral' THEN 'Ссылки на сайтах'
    WHEN `Channel`='link' THEN 'Ссылки на сайтах'
    WHEN `Source`='Link traffic' THEN 'Ссылки на сайтах'
    WHEN `Channel`='webview' THEN 'Ссылки на сайтах'
    WHEN `Source`='Recommendation system traffic' THEN 'Ссылки на сайтах'
    WHEN `Channel`='offline' THEN 'Оффлайн-реклама'
    WHEN `Channel`='listovka' THEN 'Оффлайн-реклама'
    WHEN `Channel`='buklet' THEN 'Оффлайн-реклама'
    WHEN `Channel`='talon' THEN 'Оффлайн-реклама'
    WHEN `Channel`='vizitka' THEN 'Оффлайн-реклама'
    WHEN `Channel`='cpm' THEN 'Контекстная реклама'
    WHEN `Channel`='vdo.cpm' THEN 'Контекстная реклама'
    WHEN `Channel`='rtb-cpm' THEN 'Контекстная реклама'
    WHEN `Channel`='cpc' THEN 'Контекстная реклама'
	WHEN `Channel`='cpc|DB.ru/app-2gis' THEN 'Контекстная реклама'
	WHEN `Channel`='cpc/' THEN 'Контекстная реклама'
    WHEN `Channel`='cpc (ymclid)' THEN 'Контекстная реклама'
	WHEN `Channel`='cpc (yclid)' THEN 'Контекстная реклама'
	WHEN `Channel`='cpc (gclid)' THEN 'Контекстная реклама'
    WHEN `Channel`='click' THEN 'Контекстная реклама'
	WHEN `Channel`='click_click' THEN 'Контекстная реклама'
    WHEN `Channel`='cpa' THEN 'Контекстная реклама'
    WHEN `Channel`='ad' THEN 'Контекстная реклама'
    WHEN `Source`='Ad traffic' THEN 'Контекстная реклама'
    WHEN `Channel`='banner' THEN 'Контекстная реклама'
    WHEN `Channel`='cpc,cpc' THEN 'Контекстная реклама'
    WHEN `Channel`='organic' THEN 'Поисковые системы'
    WHEN `Source`='organic' THEN 'Поисковые системы'
    WHEN `Source`='ya.ru' THEN 'Поисковые системы'
    WHEN `Source`='yandex.com' THEN 'Поисковые системы'
    WHEN `Source`='Yandex' THEN 'Поисковые системы'
    WHEN `Source`='duckduckgo.com' THEN 'Поисковые системы'
    WHEN `Channel`='Веб-сайт' THEN 'Прямые заходы'
	WHEN `Channel`='CRM-форма' THEN 'Прямые заходы'
    WHEN `Channel`='direct' THEN 'Прямые заходы'
	WHEN `Channel`='<не указано>' THEN 'Прямые заходы'
	WHEN `Channel`='<не заполнено>' THEN 'Прямые заходы'
    WHEN `Channel`='' THEN 'Прямые заходы'
    WHEN `Channel`='other' THEN 'Не определено'
	WHEN `Channel`='Лид в работе' THEN 'Не определено'
	WHEN `Channel`='avito' THEN 'Маркетплейсы'
    WHEN `Channel`='calls' THEN 'Звонки'
    WHEN `Channel`='free' THEN 'Звонки'
    WHEN `Channel`='partners' THEN 'Партнеры и рекомендации'
    ELSE IFNULL(`Channel`, 'Прямые заходы')
    END AS `_Канал`,
    CASE
    WHEN `Source`='yandex,yandex' THEN 'Яндекс.Поиск'
    WHEN `Source`='yandex-direct' THEN 'Яндекс.Поиск'
    WHEN `Source`='Yandex' THEN 'Яндекс.Поиск'
    WHEN `Source`='yandex' THEN 'Яндекс.Поиск'
    WHEN `Source`='organic' THEN `Channel`
    WHEN `Source`='Link traffic' THEN 'Другие'
    WHEN `Source`='Social network traffic' THEN 'Другие'
    WHEN `Source`='Ad traffic' THEN 'Другие'
    WHEN `Source`='Messenger traffic' THEN 'Другие'
    WHEN `Source`='Internal traffic' THEN 'Другие'
    WHEN `Source`='Direct traffic' THEN 'Другие'
    WHEN `Source`='Recommendation system traffic' THEN 'Рекомендательные системы'
    WHEN `Source`='yandex_network' THEN 'РСЯ'
	WHEN `Campaign`='rsa' THEN 'РСЯ'
    WHEN `Source`='yandex' THEN CASE WHEN `Channel`='cpc' THEN 'Яндекс.Поиск' WHEN `Channel`='cpm' THEN 'Яндекс.Поиск' ELSE CASE WHEN `Source`='' THEN 'Другие' ELSE IFNULL(`Source`, 'Другие') END END
    WHEN `Source`='google' THEN CASE WHEN `Channel`='cpc' THEN 'Google.Adwords' ELSE CASE WHEN `Source`='' THEN 'Другие' ELSE IFNULL(`Source`, 'Другие') END END
    WHEN `Source`='e.mail.ru' THEN CASE WHEN `Channel`='referral' THEN 'Почта Mail.Ru' ELSE CASE WHEN `Source`='' THEN 'Другие' ELSE IFNULL(`Source`, 'Другие') END END
    WHEN `Source`='click.mail.ru' THEN CASE WHEN `Channel`='referral' THEN 'Почта Mail.Ru' ELSE CASE WHEN `Source`='' THEN 'Другие' ELSE IFNULL(`Source`, 'Другие') END END
    WHEN `Source`='m.vk.com' THEN CASE WHEN `Channel`='referral' THEN 'Соц.сети|VK' ELSE CASE WHEN `Source`='' THEN 'Другие' ELSE IFNULL(`Source`, 'Другие') END END
    WHEN `Source`='away.vk.com' THEN CASE WHEN `Channel`='referral' THEN 'Соц.сети|VK' ELSE CASE WHEN `Source`='' THEN 'Другие' ELSE IFNULL(`Source`, 'Другие') END END
    WHEN `Source`='ya.ru' THEN CASE WHEN `Channel`='referral' THEN 'yandex' ELSE CASE WHEN `Source`='' THEN 'Другие' ELSE IFNULL(`Source`, 'Другие') END END
    WHEN `Source`='yandex.com' THEN CASE WHEN `Channel`='referral' THEN 'yandex' ELSE CASE WHEN `Source`='' THEN 'Другие' ELSE IFNULL(`Source`, 'Другие') END END
    WHEN `Source`='duckduckgo.com' THEN CASE WHEN `Channel`='referral' THEN 'duckduckgo' ELSE CASE WHEN `Source`='' THEN 'Другие' ELSE IFNULL(`Source`, 'Другие') END END
    ELSE CASE WHEN `Source`='' THEN 'Другие' ELSE IFNULL(`Source`, 'Другие') END
    END AS `_Источник`,
	CASE
		WHEN `Campaign`='<не указано>' THEN ''
		WHEN `Channel`='organic' THEN ''
		WHEN `Campaign`='rsa' THEN `Source`
		ELSE IFNULL(`Campaign`, '')
	END AS `_Кампания`

FROM (SELECT
    l.DT AS `Date`,
    0 AS `Visits`,
    0 AS `Costs`,
    0 AS `Installs`,
    SUM(l.LEADS) AS `Leads`,
    0 AS `Deals`,
    0 AS `Revenue`,
    0 AS `RepeatDeals`,
    l.UTM_MEDIUM_PURE AS `Channel`,
    l.UTM_SOURCE_PURE AS `Source`,
    l.UTM_CAMPAIGN_PURE AS `Campaign`
FROM
    DB.mart_mkt_bx_leads_app as l
GROUP BY `Channel`,`Source`,`Campaign`,`Date`

UNION ALL

SELECT
    d.DT AS `Date`,
    0 AS `Visits`,
    0 AS `Costs`,
    0 AS `Leads`,
	0 AS `Installs`,
    SUM(d.DEALS) AS `Deals`,
    SUM(d.REVENUE) AS `Revenue`,
    SUM(d.REPEATDEALS) AS `RepeatDeals`,
    d.UTM_MEDIUM_PURE AS `Channel`,
    d.UTM_SOURCE_PURE AS `Source`,
    d.UTM_CAMPAIGN_PURE AS `Campaign`
FROM
    DB.mart_mkt_bx_deals_app as d
GROUP BY `Channel`,`Source`,`Campaign`,`Date`

UNION ALL

SELECT
    v.DT AS `Date`,
    SUM(v.VISITS) AS `Visits`,
    0 AS `Costs`,
	0 AS `Installs`,
    0 AS `Leads`,
    0 AS `Deals`,
    0 AS `Revenue`,
    0 AS `RepeatDeals`,
    v.UTM_MEDIUM_PURE AS `Channel`,
    v.UTM_SOURCE_PURE AS `Source`,
    v.UTM_CAMPAIGN_PURE AS `Campaign`
FROM
    DB.mart_mkt_ym_visits as v
GROUP BY `Channel`,`Source`,`Campaign`,`Date`

UNION ALL

SELECT
    c.DT AS `Date`,
    0 AS `Visits`,
    SUM(c.COSTS) AS `Costs`,
	0 AS `Installs`,
    0 AS `Leads`,
    0 AS `Deals`,
    0 AS `Revenue`,
    0 AS `RepeatDeals`,
    c.UTM_MEDIUM_PURE AS `Channel`,
    c.UTM_SOURCE_PURE AS `Source`,
    replaceAll(c.CAMPAIGN_NAME, ' ', '_') AS `Campaign`
FROM
    DB.mart_mkt_yd_costs as c
GROUP BY `Channel`,`Source`,`Campaign`,`Date`

UNION ALL

SELECT
    DT AS `Date`,
    0 AS `Visits`,
    SUM(COSTS) AS `Costs`,
	0 AS `Installs`,
    0 AS `Leads`,
    0 AS `Deals`,
    0 AS `Revenue`,
    0 AS `RepeatDeals`,
    vk.UTM_MEDIUM_PURE AS `Channel`,
    vk.UTM_SOURCE_PURE AS `Source`,
    vk.UTM_CAMPAIGN_ID AS `Campaign`
FROM
    DB.mart_mkt_vk_costs as vk
GROUP BY `Channel`,`Source`,`Campaign`,`Date`

UNION ALL

SELECT
    DT AS `Date`,
    0 AS `Visits`,
    0 AS `Costs`,
	SUM(INSTALLS) AS `Installs`,
    0 AS `Leads`,
    0 AS `Deals`,
    0 AS `Revenue`,
    0 AS `RepeatDeals`,
    ya.UTM_MEDIUM_PURE AS `Channel`,
    ya.UTM_SOURCE_PURE AS `Source`,
    ya.UTM_CAMPAIGN_PURE AS `Campaign`
FROM
    DB.mart_mkt_ya_installs as ya
GROUP BY `Channel`,`Source`,`Campaign`,`Date`

SETTINGS join_use_nulls = 1)

GROUP BY `_Канал`,`_Источник`,`_Кампания`,`_Дата`