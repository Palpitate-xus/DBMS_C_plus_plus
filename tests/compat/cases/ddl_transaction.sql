-- CREATE TABLE and subsequent DML are undone by the outer transaction.
DROP TABLE IF EXISTS pgdiff_ddl_txn;
BEGIN;
CREATE TABLE pgdiff_ddl_txn (v INT);
INSERT INTO pgdiff_ddl_txn VALUES (1);
ROLLBACK;
SELECT * FROM pgdiff_ddl_txn;
