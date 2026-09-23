-- A failed duplicate CREATE TABLE aborts the transaction with 42P07.
DROP TABLE IF EXISTS pgdiff_ddl_error_txn;
BEGIN;
CREATE TABLE pgdiff_ddl_error_txn (v INT);
CREATE TABLE pgdiff_ddl_error_txn (v INT);
INSERT INTO pgdiff_ddl_error_txn VALUES (1);
ROLLBACK;
SELECT * FROM pgdiff_ddl_error_txn;
