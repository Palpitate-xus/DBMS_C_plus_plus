DROP TABLE IF EXISTS diff_create_mv_source;
CREATE TABLE diff_create_mv_source (id integer);
CREATE MATERIALIZED VIEW diff_create_mv_missing.mv AS SELECT id FROM diff_create_mv_source;
DROP TABLE diff_create_mv_source;
