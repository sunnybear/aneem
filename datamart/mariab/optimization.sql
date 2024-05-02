-- raw_ym_visits
alter table raw_ym_visits add index datetime (`ym:s:dateTime`);
alter table raw_ym_visits add index endurl (`ym:s:endURL`);
alter table raw_ym_visits add index utmsource (`ym:s:lastUTMSource`);
alter table raw_ym_visits add index utmmedium (`ym:s:lastUTMMedium`);
alter table raw_ym_visits add index utmcampaign (`ym:s:lastUTMCampaign`);
alter table raw_ym_visits add index clientid (`ym:s:clientID`);
alter table raw_ym_visits add index visitid (`ym:s:visitID`);
-- raw_ym_visits_goals
alter table raw_ym_visits_goals add index datetime (`ym:s:goalDateTime`);
alter table raw_ym_visits_goals add index goalid (`ym:s:goalID`);
alter table raw_ym_visits_goals add index clientid (`ym:s:clientID`);
alter table raw_ym_visits_goals add index visitid (`ym:s:visitID`);