-- ROLLBACK must restore populated state and rows after WITH NO DATA.
CREATE TABLE diff_mv_nodata_src(id integer);
INSERT INTO diff_mv_nodata_src VALUES (7);
CREATE MATERIALIZED VIEW diff_mv_nodata_txn AS SELECT id FROM diff_mv_nodata_src;
BEGIN;
SAVEPOINT diff_mv_nodata_sp;
REFRESH MATERIALIZED VIEW diff_mv_nodata_txn WITH NO DATA;
ROLLBACK TO SAVEPOINT diff_mv_nodata_sp;
SELECT id FROM diff_mv_nodata_txn;
REFRESH MATERIALIZED VIEW diff_mv_nodata_txn WITH NO DATA;
ROLLBACK;
SELECT id FROM diff_mv_nodata_txn;
DROP MATERIALIZED VIEW diff_mv_nodata_txn;
DROP TABLE diff_mv_nodata_src;
