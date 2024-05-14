CREATE VIEW DB.raw_data_status AS

(SELECT
    'YM' as `source`,
	'Яндекс.Метрика: визиты на сайт' as `title`,
    count(*) as total,
    max(`ym:s:dateTime`) as date_end,
    min(`ym:s:dateTime`) as date_start
FROM
    DB.raw_ym_visits

UNION ALL
	
SELECT
    'YMG' as `source`,
	'Яндекс.Метрика: цели визитов' as `title`,
    count(*) as total,
    max(`ym:s:goalDateTime`) as date_end,
    min(`ym:s:goalDateTime`) as date_start
FROM
    DB.raw_ym_visits_goals

/*UNION ALL

SELECT
    'AM' as `source`,
	'Яндекс.Аппметрика: установки приложения' as `title`,
    count(*) as total,
    max(`install_datetime`) as date_end,
    min(`install_datetime`) as date_start
FROM
    DB.raw_am_installs
*/
/*UNION ALL

SELECT
    'AME' as `source`,
	'Яндекс.Аппметрика: события в приложении' as `title`,
    count(*) as total,
    max(`event_datetime`) as date_end,
    min(`event_datetime`) as date_start
FROM
    DB.raw_am_events
*/
/*UNION ALL

SELECT
    'CT' as `source`,
	'Calltouch: звонки' as `title`,
    count(*) as total,
    max(`date`) as date_end,
    min(`date`) as date_start
FROM
    DB.raw_ct_calls
*/
UNION ALL

SELECT
    'BX24LEAD' as `source`,
	'Битрикс24: лиды' as `title`,
    count(*) as total,
    max(`DATE_CREATE`) as date_end,
    min(`DATE_CREATE`) as date_start
FROM
    DB.raw_bx_crm_lead

UNION ALL

SELECT
    'BX24LEADUF' as `source`,
	'Битрикс24: UF лидов' as `title`,
    count(*) as total,
    NOW() as date_end,
    NOW() as date_start
FROM
    DB.raw_bx_crm_lead_uf

UNION ALL

SELECT
    'BX24DEAL' as `source`,
	'Битрикс24: сделки' as `title`,
    count(*) as total,
    max(`DATE_CREATE`) as date_end,
    min(`DATE_CREATE`) as date_start
FROM
    DB.raw_bx_crm_deal
	
UNION ALL

SELECT
    'BX24CONTACT' as `source`,
	'Битрикс24: контакты' as `title`,
    count(*) as total,
    max(`DATE_CREATE`) as date_end,
    min(`DATE_CREATE`) as date_start
FROM
    DB.raw_bx_crm_contact

UNION ALL

SELECT
    'BX24CONTACTUF' as `source`,
	'Битрикс24: телефон и email' as `title`,
    count(*) as total,
    NOW() as date_end,
    NOW() as date_start
FROM
    DB.raw_bx_crm_contact_uf
	
UNION ALL

SELECT
    'BX24CONTACTS' as `source`,
	'Битрикс24: телефон и email' as `title`,
    count(*) as total,
    NOW() as date_end,
    NOW() as date_start
FROM
    DB.raw_bx_crm_contact_contacts
	
UNION ALL

SELECT
    'BX24COMPANY' as `source`,
	'Битрикс24: компании' as `title`,
    count(*) as total,
    max(`DATE_CREATE`) as date_end,
    min(`DATE_CREATE`) as date_start
FROM
    DB.raw_bx_crm_company
	
UNION ALL

SELECT
    'BX24STATUS' as `source`,
	'Битрикс24: словари' as `title`,
    count(*) as total,
    NOW() as date_end,
    NOW() as date_start
FROM
    DB.raw_bx_crm_status

UNION ALL

SELECT
    'BX24DEALCAT' as `source`,
	'Битрикс24: категории сделок' as `title`,
    count(*) as total,
    NOW() as date_end,
    NOW() as date_start
FROM
    DB.raw_bx_crm_category

/*UNION ALL

SELECT
    'VK' as `source`,
	'ВК: расходы' as `title`,
    count(*) as total,
	max(`date`) as date_end,
    min(`date`) as date_start
FROM
    DB.raw_vk_costs
*/

/*UNION ALL

SELECT
    'BXO' as `source`,
	'Битрикс: заказы' as `title`,
    count(*) as total,
	max(`dateInsert`) as date_end,
    min(`dateInsert`) as date_start
FROM
    DB.raw_bx_orders
*/

/*UNION ALL

SELECT
    'BXOG' as `source`,
	'Битрикс: товары в заказах' as `title`,
    count(*) as total,
	NOW() as date_end,
    NOW() as date_start
FROM
    raw_bx_orders_goods
*/

/*UNION ALL

SELECT
    '1C' as `source`,
	'1C: продажи' as `title`,
    count(*) as total,
	max(`Дата_Заказа`) as date_end,
    min(`Дата_Заказа`) as date_start
FROM
    raw_1c_sales
*/
UNION ALL

SELECT
    'YD' as `source`,
	'Яндекс.Директ: расходы' as `title`,
    count(*) as total,
	max(`Date`) as date_end,
    min(`Date`) as date_start
FROM
    DB.raw_yd_costs

UNION ALL

SELECT
    'YDU' as `source`,
	'Яндекс.Директ: UTM метки' as `title`,
    count(*) as total,
	NOW() as date_end,
    NOW() as date_start
FROM
    raw_yd_campaigns_utms)