CREATE TEMP TABLE diff_temp_namespace_after_drop (id integer);
DROP TABLE diff_temp_namespace_after_drop;
DROP INDEX pg_temp.diff_temp_namespace_missing_idx;
DROP TABLE pg_temp.diff_temp_namespace_missing_table;
CREATE INDEX ON pg_temp.diff_temp_namespace_missing_table (id);
SELECT 1;
