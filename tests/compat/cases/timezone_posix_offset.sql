-- Quoted offsets use POSIX sign convention; numeric hours use SQL convention.
DROP TABLE IF EXISTS pgdiff_tz_posix;
CREATE TABLE pgdiff_tz_posix (ts TIMESTAMPTZ);
INSERT INTO pgdiff_tz_posix VALUES ('2026-08-17 10:00:00+00');
SET TIME ZONE '+08:00';
SHOW TimeZone;
SELECT ts FROM pgdiff_tz_posix;
SET TIME ZONE '-08:00';
SHOW TimeZone;
SELECT ts FROM pgdiff_tz_posix;
SET TIME ZONE 8;
SHOW TimeZone;
SELECT ts FROM pgdiff_tz_posix;
