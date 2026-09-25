DROP MATERIALIZED VIEW IF EXISTS diff_duplicate_mv;
DROP TABLE IF EXISTS diff_duplicate_mv_source;
CREATE TABLE diff_duplicate_mv_source (id integer);
INSERT INTO diff_duplicate_mv_source VALUES (31);
CREATE MATERIALIZED VIEW diff_duplicate_mv AS SELECT id FROM diff_duplicate_mv_source;
INSERT INTO diff_duplicate_mv_source VALUES (32);
CREATE MATERIALIZED VIEW diff_duplicate_mv AS SELECT id FROM diff_duplicate_mv_source;
SELECT id FROM diff_duplicate_mv ORDER BY id;
DROP MATERIALIZED VIEW diff_duplicate_mv;
