SELECT timestamp '2026-08-15 14:30:05' AT TIME ZONE 'UTC',timestamptz '2026-08-17 10:00:00+00' AT TIME ZONE 'America/New_York','2024-06-01 00:30:00'::timestamp AT TIME ZONE 'UTC+8';
SELECT (timestamp '2026-08-15 14:30:05' AT TIME ZONE 'UTC'),timestamp '2026-08-15 14:30:05' AT TIME ZONE 'UTC' AS instant;
SELECT -1::numeric,1::numeric+2::numeric;
