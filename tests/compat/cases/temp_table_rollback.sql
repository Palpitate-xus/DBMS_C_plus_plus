BEGIN;
CREATE TEMP TABLE diff_temp_table_rollback (id integer);
ROLLBACK;
SELECT * FROM diff_temp_table_rollback;
DROP INDEX pg_temp.diff_temp_missing_idx;
SELECT 1;
