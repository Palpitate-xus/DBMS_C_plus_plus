DROP TABLE IF EXISTS diff_drop_mv_table;
CREATE TABLE diff_drop_mv_table (id integer);
INSERT INTO diff_drop_mv_table VALUES (121);
DROP MATERIALIZED VIEW diff_drop_mv_table;
DROP MATERIALIZED VIEW IF EXISTS diff_drop_mv_table;
SELECT id FROM diff_drop_mv_table;
DROP TABLE diff_drop_mv_table;
