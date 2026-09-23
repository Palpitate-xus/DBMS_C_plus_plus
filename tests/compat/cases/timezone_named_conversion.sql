-- Named zones in both timezone() overloads and AT TIME ZONE use date-specific rules.
SET TIME ZONE 'UTC';
SELECT timezone('America/New_York', timestamptz '2026-01-17 10:00:00+00'), timezone('America/New_York', timestamptz '2026-08-17 10:00:00+00');
SELECT timezone('America/New_York', timestamp '2026-01-17 10:00:00'), timezone('America/New_York', timestamp '2026-08-17 10:00:00');
SELECT timestamptz '2026-08-17 10:00:00+00' AT TIME ZONE 'America/New_York';
SELECT timestamp '2026-08-17 10:00:00' AT TIME ZONE 'America/New_York';
SELECT timezone('Europe/London', timestamptz '2026-08-17 10:00:00+00');
SELECT timezone('Asia/Osaka', timestamp '2026-08-17 10:00:00');
SELECT timestamp '2026-08-17 10:00:00' AT TIME ZONE 'Asia/Osaka';
