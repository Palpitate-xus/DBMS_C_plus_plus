BEGIN;
SAVEPOINT temp_create_sp;
CREATE TEMP TABLE diff_temp_create_savepoint (id integer);
ROLLBACK TO SAVEPOINT temp_create_sp;
CREATE TEMP TABLE diff_temp_create_savepoint (id integer);
COMMIT;
DROP TABLE diff_temp_create_savepoint;
SELECT 1;
