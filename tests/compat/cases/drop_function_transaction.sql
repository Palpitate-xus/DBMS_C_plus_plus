-- Function drops and IF EXISTS must not commit the caller's transaction.
DROP FUNCTION IF EXISTS pgdiff_drop_tx_fn();
DROP FUNCTION pgdiff_missing_drop_tx_fn();
DROP TABLE IF EXISTS pgdiff_drop_tx_table;
CREATE FUNCTION pgdiff_drop_tx_fn() RETURNS int LANGUAGE SQL AS $$ SELECT 7 $$;
BEGIN;
DROP FUNCTION pgdiff_drop_tx_fn();
ROLLBACK;
SELECT pgdiff_drop_tx_fn();
BEGIN;
CREATE TABLE pgdiff_drop_tx_table (a INT);
DROP FUNCTION IF EXISTS pgdiff_missing_drop_tx_fn();
ROLLBACK;
SELECT a FROM pgdiff_drop_tx_table;
