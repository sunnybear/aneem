-- raw_ym_visits
IF EXISTS(SELECT raw_ym_visits FROM INFORMATION_SCHEMA.TABLES WHERE table_name LIKE 'raw_ym_visits') THEN
	alter table raw_ym_visits add index datetime (`ym:s:dateTime`);
	alter table raw_ym_visits add index endurl (`ym:s:endURL`);
	alter table raw_ym_visits add index utmsource (`ym:s:lastUTMSource`);
	alter table raw_ym_visits add index utmmedium (`ym:s:lastUTMMedium`);
	alter table raw_ym_visits add index utmcampaign (`ym:s:lastUTMCampaign`);
	alter table raw_ym_visits add index clientid (`ym:s:clientID`);
	alter table raw_ym_visits add index visitid (`ym:s:visitID`);
END IF;
-- raw_ym_visits_goals
IF EXISTS(SELECT raw_ym_visits_goals FROM INFORMATION_SCHEMA.TABLES WHERE table_name LIKE 'raw_ym_visits_goals') THEN
	alter table raw_ym_visits_goals add index datetime (`ym:s:goalDateTime`);
	alter table raw_ym_visits_goals add index goalid (`ym:s:goalID`);
	alter table raw_ym_visits_goals add index clientid (`ym:s:clientID`);
	alter table raw_ym_visits_goals add index visitid (`ym:s:visitID`);
END IF;
-- raw_ym_costs
IF EXISTS(SELECT raw_ym_costs FROM INFORMATION_SCHEMA.TABLES WHERE table_name LIKE 'raw_ym_costs') THEN
	alter table raw_ym_costs add index datetime (`ym:ev:date`);
	alter table raw_ym_costs add index utmsource (`ym:s:lastExpenseSource`);
	alter table raw_ym_costs add index utmmedium (`ym:s:lastExpenseMedium`);
	alter table raw_ym_costs add index utmcampaign (`ym:s:lastExpenseCampaign`);
END IF;
-- raw_bx_orders
IF EXISTS(SELECT raw_bx_orders FROM INFORMATION_SCHEMA.TABLES WHERE table_name LIKE 'raw_bx_orders') THEN
	alter table raw_bx_orders add index dateinsert (`dateInsert`);
	alter table raw_bx_orders add index dateupdate (`dateUpdate`);
	alter table raw_bx_orders add index id (`id`);
	alter table raw_bx_orders add index statusid (`statusId`);
END IF;
-- raw_bx_orders_goods
IF EXISTS(SELECT raw_bx_orders_goods FROM INFORMATION_SCHEMA.TABLES WHERE table_name LIKE 'raw_bx_orders_goods') THEN
	alter table raw_bx_orders_goods add index orderid (`orderId`);
	alter table raw_bx_orders_goods add index price (`price`);
	alter table raw_bx_orders_goods add index quantity (`quantity`);
END IF;
-- raw_yd_costs
IF EXISTS(SELECT raw_yd_costs FROM INFORMATION_SCHEMA.TABLES WHERE table_name LIKE 'raw_yd_costs') THEN
	alter table raw_yd_costs add index dateidx (`Date`);
	alter table raw_yd_costs add index campaign (`CampaignId`);
	alter table raw_yd_costs add index networktype (`AdNetworkType`);
END IF;
-- raw_yd_campaigns_utms
IF EXISTS(SELECT raw_yd_campaigns_utms FROM INFORMATION_SCHEMA.TABLES WHERE table_name LIKE 'raw_yd_campaigns_utms') THEN
	alter table raw_yd_campaigns_utms add index campaignid (`CampaignId`);
	alter table raw_yd_campaigns_utms add index medium (`UTMMedium`);
	alter table raw_yd_campaigns_utms add index source (`UTMSource`);
	alter table raw_yd_campaigns_utms add index campaign (`UTMCampaign`);
END IF;