-- A legacy fixed-offset alias must not bypass IANA name validation.
SET TIME ZONE 'UTC';
SET TIME ZONE 'Asia/Osaka';
SHOW TimeZone;
SET TIME ZONE 'America/Miami';
SHOW TimeZone;
SET TIME ZONE 'America/Seattle';
SHOW TimeZone;
SET TIME ZONE 'Z';
SHOW TimeZone;
SET TIME ZONE 'america/new_york';
SHOW TimeZone;
SELECT '2026-08-17 10:00:00+00'::timestamptz;
SET TIME ZONE 'us/eastern';
SHOW TimeZone;
SELECT '2026-08-17 10:00:00+00'::timestamptz;
SET TIME ZONE 'Europe/Kyiv';
SHOW TimeZone;
SET TIME ZONE 'UTC';
