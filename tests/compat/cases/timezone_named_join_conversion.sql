-- JOIN projections must preserve both timezone() overloads and result types.
DROP TABLE IF EXISTS pgdiff_tz_join_a;
DROP TABLE IF EXISTS pgdiff_tz_join_b;
CREATE TABLE pgdiff_tz_join_a (id INT, instant TIMESTAMPTZ);
CREATE TABLE pgdiff_tz_join_b (id INT, wall TIMESTAMP);
INSERT INTO pgdiff_tz_join_a VALUES (1, '2026-01-17 10:00:00+00'), (2, '2026-08-17 10:00:00+00');
INSERT INTO pgdiff_tz_join_b VALUES (1, '2026-01-17 10:00:00'), (2, '2026-08-17 10:00:00');
SET TIME ZONE 'UTC';
SELECT a.id, timezone('America/New_York', a.instant), timezone('America/New_York', b.wall) FROM pgdiff_tz_join_a a JOIN pgdiff_tz_join_b b ON a.id = b.id ORDER BY a.id;
SELECT a.id, a.instant AT TIME ZONE 'America/New_York', b.wall AT TIME ZONE 'America/New_York' FROM pgdiff_tz_join_a a JOIN pgdiff_tz_join_b b ON a.id = b.id ORDER BY a.id;
SELECT timezone('America/New_York', a.instant) AS local_instant FROM pgdiff_tz_join_a a JOIN pgdiff_tz_join_b b ON a.id = b.id ORDER BY a.id;
