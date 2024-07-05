/* Цепочка касаний из всех источников трафика с номером телефона и email */
/*
	Дата касания: touchDateTime,
	Тип касания: touchType,
	ID касания (в источнике): touchID,
	Источник касания: touchSource,
	Название в источнике касания: touchSourceName,
	Телефон: phone,
	Email: email,
	Канал касания: UTMMedium,
	Источник касания: UTMSource,
	Кампания касания: UTMCampaign,
	Ключевое слово касания: UTMTerm,
	Содержание касания: UTMContent */
/* необходимо выбрать используемые источники */

CREATE OR REPLACE VIEW int_mart_e2e_touches_phone AS
SELECT
        CASE
            WHEN ct_yclid.phone<>'' THEN ct_yclid.phone
            WHEN amiid_phone.phone<>'' THEN amiid_phone.phone
            ELSE t.phone
        END AS phone,
		email,
        touchType,
        touchSource,
        touchSourceName,
        touchDateTime,
        touchID,
        UTMMedium,
        UTMSource,
        UTMCampaign,
        UTMTerm,
        UTMContent
    FROM
        int_mart_e2e_touches AS t
    LEFT ANY JOIN dict_metrika_amiid_yclid AS amiid_yclid ON t.yclid=amiid_yclid.yclid
    LEFT ANY JOIN dict_appmetrica_amiid_phone_hash_all AS amiid_phone ON amiid_yclid.amiid=amiid_phone.amiid
    LEFT ANY JOIN dict_calltouch_phone_yclid AS ct_yclid ON t.yclid=ct_yclid.yclid
	WHERE phone<>'';