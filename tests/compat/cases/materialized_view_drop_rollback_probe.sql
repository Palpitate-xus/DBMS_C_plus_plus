-- Both savepoint and top-level rollback must restore a materialized view.
CREATE TABLE diff_mv_drop_src(id integer);
INSERT INTO diff_mv_drop_src VALUES (7);
CREATE MATERIALIZED VIEW diff_mv_drop_txn AS SELECT id FROM diff_mv_drop_src;
BEGIN;
SAVEPOINT diff_mv_drop_sp;
DROP MATERIALIZED VIEW diff_mv_drop_txn;
ROLLBACK TO SAVEPOINT diff_mv_drop_sp;
SELECT id FROM diff_mv_drop_txn;
DROP MATERIALIZED VIEW diff_mv_drop_txn;
ROLLBACK;
SELECT id FROM diff_mv_drop_txn;
DROP MATERIALIZED VIEW diff_mv_drop_txn;
DROP TABLE diff_mv_drop_src;
