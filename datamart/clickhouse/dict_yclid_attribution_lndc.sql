-- 1. ground table
CREATE TABLE DB.dict_yclid_attribution_lndc
(
    `yclid` String,
    `UTM_MEDIUM` String,
    `UTM_SOURCE` String,
	`UTM_CAMPAIGN` String,
	`UTM_TERM` String
)
ENGINE = SummingMergeTree
ORDER BY (yclid, UTM_MEDIUM, UTM_SOURCE, UTM_CAMPAIGN, UTM_TERM);

-- 2. materialized view (updates data rom now)
DROP VIEW IF EXISTS DB.dict_yclid_attribution_lndc_mv;
CREATE MATERIALIZED VIEW DB.dict_yclid_attribution_lndc_mv TO DB.dict_yclid_attribution_lndc AS
SELECT
	yclid,
	UTM_MEDIUM,
	UTM_SOURCE,
	UTM_CAMPAIGN,
	UTM_TERM
FROM (SELECT
    `ym:s:clientID` as yclid,
    ROW_NUMBER() OVER (PARTITION BY `ym:s:dateTime` ORDER BY `ym:s:dateTime`) AS rowNum,
    CASE 
        WHEN `ym:s:lastUTMMedium`='' THEN `ym:s:lastTrafficSource`
        ELSE `ym:s:lastUTMMedium`
    END as UTM_MEDIUM,
    CASE
        WHEN `ym:s:lastUTMMedium`='' THEN CASE WHEN `ym:s:lastTrafficSource`='organic' THEN `ym:s:lastSearchEngine` WHEN `ym:s:lastTrafficSource`='referral' THEN `ym:s:lastReferalSource` WHEN `ym:s:lastTrafficSource`='ad' THEN `ym:s:lastAdvEngine` WHEN `ym:s:lastTrafficSource`='social' THEN `ym:s:lastSocialNetwork` WHEN `ym:s:lastTrafficSource`='messenger' THEN `ym:s:lastMessenger` ELSE `ym:s:from` END
        ELSE `ym:s:lastUTMSource`
    END as UTM_SOURCE,
    `ym:s:lastUTMCampaign` as UTM_CAMPAIGN,
	`ym:s:lastUTMTerm` as UTM_TERM
FROM florcat.raw_ym_visits
WHERE `ym:s:visitDuration`>5
AND ((`ym:s:lastUTMMedium`<>'' AND `ym:s:lastUTMMedium`<>'direct' AND `ym:s:lastUTMMedium`<>'internal') OR (`ym:s:lastTrafficSource`<>'' AND `ym:s:lastTrafficSource`<>'direct' AND `ym:s:lastTrafficSource`<>'internal'))
) WHERE rowNum=1;

-- 3. initial data upload
INSERT INTO DB.dict_yclid_attribution_lndc SELECT
	yclid,
	UTM_MEDIUM,
	UTM_SOURCE,
	UTM_CAMPAIGN,
	UTM_TERM
FROM (SELECT
    `ym:s:clientID` as yclid,
    ROW_NUMBER() OVER (PARTITION BY `ym:s:dateTime` ORDER BY `ym:s:dateTime`) AS rowNum,
    CASE 
        WHEN `ym:s:lastUTMMedium`='' THEN `ym:s:lastTrafficSource`
        ELSE `ym:s:lastUTMMedium`
    END as UTM_MEDIUM,
    CASE
        WHEN `ym:s:lastUTMMedium`='' THEN CASE WHEN `ym:s:lastTrafficSource`='organic' THEN `ym:s:lastSearchEngine` WHEN `ym:s:lastTrafficSource`='referral' THEN `ym:s:lastReferalSource` WHEN `ym:s:lastTrafficSource`='ad' THEN `ym:s:lastAdvEngine` WHEN `ym:s:lastTrafficSource`='social' THEN `ym:s:lastSocialNetwork` WHEN `ym:s:lastTrafficSource`='messenger' THEN `ym:s:lastMessenger` ELSE `ym:s:from` END
        ELSE `ym:s:lastUTMSource`
    END as UTM_SOURCE,
    `ym:s:lastUTMCampaign` as UTM_CAMPAIGN,
	`ym:s:lastUTMTerm` as UTM_TERM
FROM florcat.raw_ym_visits
WHERE `ym:s:visitDuration`>5
AND ((`ym:s:lastUTMMedium`<>'' AND `ym:s:lastUTMMedium`<>'direct' AND `ym:s:lastUTMMedium`<>'internal') OR (`ym:s:lastTrafficSource`<>'' AND `ym:s:lastTrafficSource`<>'direct' AND `ym:s:lastTrafficSource`<>'internal'))
) WHERE rowNum=1;