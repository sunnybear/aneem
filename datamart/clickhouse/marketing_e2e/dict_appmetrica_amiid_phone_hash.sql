/* Словарь соответствия phone-amiid для Yandex.Appmetrica (из JSON-тел событий с полем key - md5hash для телефона) */
/*
	Yandex Appmetrica Installation ID: amiid,
	Телефон: phone */
-- 1. целевая таблица
CREATE OR REPLACE TABLE dict_appmetrica_amiid_phone_hash
(
    `amiid` String,
    `phone` String,
)
ENGINE = SummingMergeTree
ORDER BY (amiid, phone);

-- 2. материализованное представление (триггер на обновление данных целевой таблицы)
DROP VIEW IF EXISTS dict_appmetrica_amiid_phone_hash_mv;
CREATE MATERIALIZED VIEW dict_appmetrica_amiid_phone_hash_mv TO dict_appmetrica_amiid_phone_hash AS
SELECT
    e.installation_id as amiid,
    CONCAT('7', SUBSTRING(replace(replace(replace(replace(replace(`phone1`, '(', ''), ')', ''), ' ', ''), '+', ''), '-', ''), 2)) AS phone
FROM raw_ya_events as e
	LEFT JOIN raw_bx_crm_contact_uf as c ON lower(hex(MD5(CONCAT('+', CONCAT('7', SUBSTRING(replace(replace(replace(replace(replace(`phone1`, '(', ''), ')', ''), ' ', ''), '+', ''), '-', ''), 2))))))=replaceAll(replaceAll(simpleJSONExtractRaw(e.event_json, 'key'), '\"', ''), '+', '')
WHERE c.phone1 != '';

-- 3. загрузка исходных данных
INSERT INTO dict_appmetrica_amiid_phone_hash SELECT
    e.installation_id as amiid,
    CONCAT('7', SUBSTRING(replace(replace(replace(replace(replace(`phone1`, '(', ''), ')', ''), ' ', ''), '+', ''), '-', ''), 2)) AS phone
FROM raw_ya_events as e
	LEFT JOIN raw_bx_crm_contact_uf as c ON lower(hex(MD5(CONCAT('+', CONCAT('7', SUBSTRING(replace(replace(replace(replace(replace(`phone1`, '(', ''), ')', ''), ' ', ''), '+', ''), '-', ''), 2))))))=replaceAll(replaceAll(simpleJSONExtractRaw(e.event_json, 'key'), '\"', ''), '+', '')
WHERE c.phone1 != '';

-- 4. дополнительное представление для объединения обычных и хэшированных телефонов
DROP VIEW IF EXISTS dict_appmetrica_amiid_phone_hash_all;
CREATE VIEW dict_appmetrica_amiid_phone_hash_all AS
SELECT * FROM (
	SELECT
		amiid,
		phone
	FROM dict_appmetrica_amiid_phone
UNION ALL
SELECT
		amiid,
		phone
	FROM dict_appmetrica_amiid_phone_hash
) GROUP BY
	amiid,
	phone;