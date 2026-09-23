-- Spring gaps and autumn overlaps need PostgreSQL-compatible resolution.
SET TIME ZONE 'America/New_York';
SELECT '2026-03-08 02:30:00'::timestamptz, '2026-11-01 01:30:00'::timestamptz;
SELECT to_timestamp('2026-03-08 02:30:00', 'YYYY-MM-DD HH24:MI:SS'), to_timestamp('2026-11-01 01:30:00', 'YYYY-MM-DD HH24:MI:SS');
SET TIME ZONE 'Europe/London';
SELECT '2026-03-29 01:30:00'::timestamptz, '2026-10-25 01:30:00'::timestamptz;
SET TIME ZONE 'UTC';
