-- misc operators: position, similar to, regex, at time zone
SELECT position('ll' in 'hello')
SELECT 'abc' similar to 'a_c'
SELECT 'abc' ~ 'a.c'
SELECT 'abc' !~ 'x.y'
SELECT 'ABC' ~* 'abc'
SELECT 'ABC' !~* 'xyz'
SELECT timestamp '2026-08-15 14:30:05' at time zone 'UTC'
SELECT timestamp '2026-08-15 14:30:05' at time zone 'Asia/Tokyo'
SELECT '2024-06-01 00:30:00'::timestamp at time zone 'UTC+8'
SELECT '2024-06-01 10:00:00'::timestamp at time zone 'UTC-05:30'
SELECT '2024-06-01 00:30:00'::timestamp at time zone '+05:30'
SELECT timezone('UTC+8', '2024-06-01 00:30:00')
