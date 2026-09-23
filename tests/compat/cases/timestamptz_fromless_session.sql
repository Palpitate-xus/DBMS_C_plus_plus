-- FROM-less timestamptz values must use the current session timezone.
SET TIME ZONE 'Asia/Shanghai';
SELECT '2026-08-17 10:00:00+00'::timestamptz;
SELECT timestamptz '2026-08-17 10:00:00+00';
SET TIME ZONE 'Asia/Kolkata';
SELECT '2026-08-17 10:00:00.123456+00'::timestamptz;
SET TIME ZONE 'UTC';
SELECT '2026-08-17 10:00:00+00'::timestamptz;
