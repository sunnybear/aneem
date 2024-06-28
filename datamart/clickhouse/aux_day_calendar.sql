CREATE OR REPLACE VIEW DB.aux_day_calendar AS
SELECT
	YEAR(DT) as Year,
	MONTH(DT) as Month,
	DAY(DT) as Day,
	toDayOfWeek(DT) as DayOfWeek,
	CASE
		WHEN DayOfWeek=1 THEN 'Понедельник'
		WHEN DayOfWeek=2 THEN 'Вторник'
		WHEN DayOfWeek=3 THEN 'Среда'
		WHEN DayOfWeek=4 THEN 'Четверг'
		WHEN DayOfWeek=5 THEN 'Пятница'
		WHEN DayOfWeek=6 THEN 'Суббота'
		WHEN DayOfWeek=7 THEN 'Воскресеньк'
	END AS DayOfWeek_RU,
	CASE
		WHEN DayOfWeek=1 THEN 'Пн'
		WHEN DayOfWeek=2 THEN 'Вт'
		WHEN DayOfWeek=3 THEN 'Ср'
		WHEN DayOfWeek=4 THEN 'Чт'
		WHEN DayOfWeek=5 THEN 'Пт'
		WHEN DayOfWeek=6 THEN 'Сб'
		WHEN DayOfWeek=7 THEN 'Вс'
	END AS DayOfWeekShort_RU,
	toWeek(DT) as Week,
	CASE
		WHEN Month<10 THEN CONCAT(toString(Year), '-0', toString(Month))
		ELSE CONCAT(toString(Year), '-', toString(Month))
	END AS YearMonth,
	CASE
		WHEN Day<10 THEN CONCAT(YearMonth, '-0', toString(Day))
		ELSE CONCAT(YearMonth, '-', toString(Day))
	END AS YearMonthDay,
	CASE
		WHEN Month=1 THEN 'Январь'
		WHEN Month=2 THEN 'Февраль'
		WHEN Month=3 THEN 'Март'
		WHEN Month=4 THEN 'Апрель'
		WHEN Month=5 THEN 'Май'
		WHEN Month=6 THEN 'Июнь'
		WHEN Month=7 THEN 'Июль'
		WHEN Month=8 THEN 'Август'
		WHEN Month=9 THEN 'Сентябрь'
		WHEN Month=10 THEN 'Октябрь'
		WHEN Month=11 THEN 'Ноябрь'
		WHEN Month=12 THEN 'Декабрь'
	END AS Month_RU,
	CASE
		WHEN Month=1 THEN 'Янв'
		WHEN Month=2 THEN 'Фев'
		WHEN Month=3 THEN 'Март'
		WHEN Month=4 THEN 'Апр'
		WHEN Month=5 THEN 'Май'
		WHEN Month=6 THEN 'Июнь'
		WHEN Month=7 THEN 'Июль'
		WHEN Month=8 THEN 'Авг'
		WHEN Month=9 THEN 'Сент'
		WHEN Month=10 THEN 'Окт'
		WHEN Month=11 THEN 'Нояб'
		WHEN Month=12 THEN 'Дек'
	END AS MonthShort_RU,
	DT
FROM (
	SELECT arrayJoin(groupArray(
		toDate('2000-01-01') + INTERVAL number DAY
	)) as DT
	FROM numbers(
		toUInt64(
			dateDiff(
				'day',
				toDate('2000-01-01'),
				toDate(NOW())
			) + 1
		)
	));