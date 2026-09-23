-- Named zones must use the offset at each timestamp, not a fixed SET-time offset.
DROP TABLE IF EXISTS pgdiff_tz_named_dst;
CREATE TABLE pgdiff_tz_named_dst (id INT, ts TIMESTAMPTZ);
INSERT INTO pgdiff_tz_named_dst VALUES (1, '2026-01-17 10:00:00+00'), (2, '2026-08-17 10:00:00+00');
SET TIME ZONE 'America/New_York';
SELECT id, ts FROM pgdiff_tz_named_dst ORDER BY id;
SELECT '2026-01-17 10:00:00'::timestamptz, '2026-08-17 10:00:00'::timestamptz;
SELECT to_timestamp('2026-01-17 10:00:00', 'YYYY-MM-DD HH24:MI:SS'), to_timestamp('2026-08-17 10:00:00', 'YYYY-MM-DD HH24:MI:SS');
SET TIME ZONE 'Europe/London';
SELECT id, ts FROM pgdiff_tz_named_dst ORDER BY id;
SELECT '2026-01-17 10:00:00+00'::timestamptz, '2026-08-17 10:00:00+00'::timestamptz;
SET TIME ZONE 'Europe/Kyiv';
SELECT id, ts FROM pgdiff_tz_named_dst ORDER BY id;
SET TIME ZONE 'UTC';
