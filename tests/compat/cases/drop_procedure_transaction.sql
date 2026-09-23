-- Procedure drops and IF EXISTS must not commit the caller's transaction.
DROP PROCEDURE IF EXISTS pgdiff_drop_tx_proc();
DROP TABLE IF EXISTS pgdiff_drop_proc_tx_table;
CREATE PROCEDURE pgdiff_drop_tx_proc() LANGUAGE SQL AS $$ SELECT 1 $$;
BEGIN;
DROP PROCEDURE pgdiff_drop_tx_proc();
ROLLBACK;
DROP PROCEDURE pgdiff_drop_tx_proc();
BEGIN;
CREATE TABLE pgdiff_drop_proc_tx_table (a INT);
DROP PROCEDURE IF EXISTS pgdiff_missing_drop_tx_proc();
ROLLBACK;
SELECT a FROM pgdiff_drop_proc_tx_table;
