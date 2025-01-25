CREATE OR REPLACE VIEW DB.aux_week_calendar AS
SELECT
	YEAR(DT) as Year,
	toWeek(DT) as Week,
	CONCAT(toString(Year), '-', toString(Week)) AS YearWeek,
	DT
FROM (
	SELECT arrayJoin(groupArray(
		toDate('2000-01-01') + INTERVAL number WEEK
	)) as DT
	FROM numbers(
		toUInt64(
			dateDiff(
				'week',
				toDate('2000-01-01'),
				toDate(NOW())
			) + 1
		)
	))