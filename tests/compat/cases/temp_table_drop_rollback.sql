CREATE TEMP TABLE diff_temp_drop_rollback (id integer);
INSERT INTO diff_temp_drop_rollback VALUES (7);
BEGIN;
DROP TABLE diff_temp_drop_rollback;
ROLLBACK;
SELECT id FROM diff_temp_drop_rollback;
DROP TABLE diff_temp_drop_rollback;
SELECT 1;
