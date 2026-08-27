-- justify_hours/days/interval
SELECT justify_hours(interval '25 hours')
SELECT justify_hours(interval '-25 hours')
SELECT justify_days(interval '70 days')
SELECT justify_days(interval '3 days 5 hours')
SELECT justify_interval(interval '-2 mons -3 days 04:05:06')
