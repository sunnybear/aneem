/* —ловарь соответстви€ phone-amiid дл€ Yandex.Appmetrica (из JSON-тел событий с полем phone) */
/*
	Yandex Appmetrica Installation ID: amiid,
	“елефон: phone */
-- 1. целева€ таблица
CREATE OR REPLACE TABLE dict_appmetrica_amiid_phone
(
    `amiid` String,
    `phone` String,
)
ENGINE = SummingMergeTree
ORDER BY (amiid, phone);

-- 2. материализованное представление (триггер на обновление данных целевой таблицы)
DROP VIEW IF EXISTS dict_appmetrica_amiid_phone_mv;
CREATE MATERIALIZED VIEW dict_appmetrica_amiid_phone_mv TO dict_appmetrica_amiid_phone AS
SELECT
    installation_id AS amiid,
    replaceAll(replaceAll(simpleJSONExtractRaw(event_json, 'phone'), '\"', ''), '+', '') AS phone
FROM raw_ya_events
WHERE phone != '';

-- 3. загрузка исходных данных
INSERT INTO dict_appmetrica_amiid_phone SELECT
    installation_id AS amiid,
    replaceAll(replaceAll(simpleJSONExtractRaw(event_json, 'phone'), '\"', ''), '+', '') AS phone
FROM raw_ya_events
WHERE phone != '';