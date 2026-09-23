-- Table-column timezone conversions may take a different projection path.
DROP TABLE IF EXISTS pgdiff_tz_conversion;
CREATE TABLE pgdiff_tz_conversion (id INT, instant TIMESTAMPTZ, wall TIMESTAMP);
INSERT INTO pgdiff_tz_conversion VALUES (1, '2026-01-17 10:00:00+00', '2026-01-17 10:00:00'), (2, '2026-08-17 10:00:00+00', '2026-08-17 10:00:00'), (3, NULL, NULL);
SET TIME ZONE 'UTC';
SELECT id, timezone('America/New_York', instant), timezone('America/New_York', wall) FROM pgdiff_tz_conversion ORDER BY id;
SELECT id, instant AT TIME ZONE 'America/New_York', wall AT TIME ZONE 'America/New_York' FROM pgdiff_tz_conversion ORDER BY id;
SELECT timezone('Asia/Osaka', instant) FROM pgdiff_tz_conversion WHERE id = 1;
SELECT instant AT TIME ZONE 'Asia/Osaka' FROM pgdiff_tz_conversion WHERE id = 1;
