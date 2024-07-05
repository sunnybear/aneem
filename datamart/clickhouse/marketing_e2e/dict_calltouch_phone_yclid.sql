/* Словарь соответствия phone-yclid для Calltouch */
/*
	Yandex Client ID: yclid,
	Calltouch ID: ctid,
	Телефон: phone */
-- 1. целевая таблица
CREATE OR REPLACE TABLE dict_calltouch_phone_yclid
(
    `phone` String,
    `yclid` String,
	`ctid` String,
)
ENGINE = SummingMergeTree
ORDER BY (phone, yclid, ctid);

-- 2. материализованное представление (триггер на обновление данных целевой таблицы)
DROP VIEW IF EXISTS dict_calltouch_phone_yclid_mv;
CREATE MATERIALIZED VIEW dict_calltouch_phone_yclid_mv TO dict_calltouch_phone_yclid AS
SELECT
	callerNumber AS phone,
	yaClientId AS yclid,
	clientId AS ctid
FROM raw_ct_calls
WHERE yclid<>'';

-- 3. загрузка исходных данных
INSERT INTO dict_calltouch_phone_yclid SELECT
	callerNumber AS phone,
	yaClientId AS yclid,
	clientId AS ctid
FROM raw_ct_calls
WHERE yclid<>'';