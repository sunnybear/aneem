-- raw_ym_visits
alter table raw_ym_visits add index datetime (`ym:s:dateTime`);
alter table raw_ym_visits add index endurl (`ym:s:endURL`);
alter table raw_ym_visits add index utmsource (`ym:s:lastUTMSource`);
alter table raw_ym_visits add index utmmedium (`ym:s:lastUTMMedium`);
alter table raw_ym_visits add index utmcampaign (`ym:s:lastUTMCampaign`);
alter table raw_ym_visits add index clientid (`ym:s:clientID`);