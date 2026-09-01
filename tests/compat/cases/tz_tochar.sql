SELECT to_char(interval '3 days 2 hours', 'HH24:MI');
SELECT to_char(interval '90 minutes', 'HH24:MI:SS');
SELECT timezone('UTC', timestamptz '2026-06-20 12:00:00');
SELECT timestamptz '2026-06-20 12:00:00' at time zone 'UTC';
SELECT timezone('UTC', timestamp '2026-06-20 12:00:00');
SELECT timestamp '2026-06-20 12:00:00' at time zone 'UTC';
SELECT to_char(timestamp '2026-06-20 12:34:56', 'HH24:MI:SS');
