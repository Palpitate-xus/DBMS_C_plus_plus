-- DROP ROUTINE resolves functions and procedures without committing a transaction.
DROP ROUTINE IF EXISTS pgdiff_drop_rt_fn();
DROP PROCEDURE IF EXISTS pgdiff_drop_rt_proc();
DROP TABLE IF EXISTS pgdiff_drop_rt_table;
CREATE FUNCTION pgdiff_drop_rt_fn() RETURNS int LANGUAGE SQL AS $$ SELECT 7 $$;
CREATE PROCEDURE pgdiff_drop_rt_proc() LANGUAGE SQL AS $$ SELECT 1 $$;
BEGIN;
DROP ROUTINE pgdiff_drop_rt_fn();
DROP ROUTINE pgdiff_drop_rt_proc();
ROLLBACK;
SELECT pgdiff_drop_rt_fn();
DROP ROUTINE pgdiff_drop_rt_fn();
DROP ROUTINE pgdiff_drop_rt_proc();
BEGIN;
CREATE TABLE pgdiff_drop_rt_table (a INT);
DROP ROUTINE IF EXISTS pgdiff_missing_rt();
ROLLBACK;
SELECT a FROM pgdiff_drop_rt_table;
