-- Whole-hour, fractional-hour, and UTC timestamptz text output.
DROP TABLE IF EXISTS pgdiff_tz_output_format;
CREATE TABLE pgdiff_tz_output_format (id INT, ts TIMESTAMPTZ);
INSERT INTO pgdiff_tz_output_format VALUES (1, '2026-08-17 10:00:00+00');
SET TIME ZONE 'Asia/Shanghai';
SELECT id, ts FROM pgdiff_tz_output_format;
SET TIME ZONE 'Asia/Kolkata';
SELECT id, ts FROM pgdiff_tz_output_format;
SET TIME ZONE 'UTC';
SELECT id, ts FROM pgdiff_tz_output_format;
