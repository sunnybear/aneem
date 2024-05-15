-- 1. ground table
CREATE TABLE DB.dict_ctphone_attribution_lndc
(
    `phone` String,
    `UTM_MEDIUM` String,
	`UTM_SOURCE` String,
	`UTM_CAMPAIGN` String
)
ENGINE = SummingMergeTree
ORDER BY (phone, UTM_MEDIUM, UTM_SOURCE, UTM_CAMPAIGN);

-- 2. materialized view (updates data rom now)
CREATE MATERIALIZED VIEW DB.dict_ctphone_attribution_lndc_mv TO DB.dict_ctphone_attribution_lndc AS
SELECT
	phone,
	UTM_MEDIUM,
	UTM_SOURCE,
	UTM_CAMPAIGN
FROM (SELECT
	callerNumber as phone,
	utmMedium as UTM_MEDIUM,
	utmSource as UTM_SOURCE,
	utmCampaign as UTM_CAMPAIGN,
	ROW_NUMBER() OVER (PARTITION BY `date` ORDER BY `date` DESC) AS rowNum
FROM DB.raw_ct_calls
WHERE
	utmMedium<>'' AND
	utmMedium<>'<не указано>' AND
	utmMedium<>'<не заполнено>' AND
	callerNumber>0)
WHERE rowNum=1;

-- 3. initial data upload
INSERT INTO DB.dict_ctphone_attribution_lndc SELECT
	phone,
	UTM_MEDIUM,
	UTM_SOURCE,
	UTM_CAMPAIGN
FROM (SELECT
	callerNumber as phone,
	utmMedium as UTM_MEDIUM,
	utmSource as UTM_SOURCE,
	utmCampaign as UTM_CAMPAIGN,
	ROW_NUMBER() OVER (PARTITION BY `date` ORDER BY `date` DESC) AS rowNum
FROM DB.raw_ct_calls
WHERE
	utmMedium<>'' AND
	utmMedium<>'<не указано>' AND
	utmMedium<>'<не заполнено>' AND
	callerNumber>0)
WHERE rowNum=1;