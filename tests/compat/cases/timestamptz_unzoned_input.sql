-- An unzoned temporal input is interpreted in the session TimeZone.
SET TIME ZONE 'Asia/Shanghai';
SELECT '2026-08-17 10:00:00'::timestamptz;
SELECT timestamp '2026-08-17 10:00:00'::timestamptz;
SELECT date '2026-08-17'::timestamptz;
SELECT to_timestamp('2026-08-17 10:00:00', 'YYYY-MM-DD HH24:MI:SS');
SET TIME ZONE 'UTC';
SELECT '2026-08-17 10:00:00'::timestamptz;
