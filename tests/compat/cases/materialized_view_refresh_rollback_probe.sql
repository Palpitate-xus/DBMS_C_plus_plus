-- A rolled-back refresh must not expose the newly refreshed rows.
CREATE TABLE diff_mv_refresh_src(id integer);
INSERT INTO diff_mv_refresh_src VALUES (1);
CREATE MATERIALIZED VIEW diff_mv_refresh_txn AS SELECT id FROM diff_mv_refresh_src;
INSERT INTO diff_mv_refresh_src VALUES (2);
BEGIN;
REFRESH MATERIALIZED VIEW diff_mv_refresh_txn;
SAVEPOINT diff_mv_refresh_sp;
INSERT INTO diff_mv_refresh_src VALUES (3);
REFRESH MATERIALIZED VIEW diff_mv_refresh_txn;
ROLLBACK TO SAVEPOINT diff_mv_refresh_sp;
SELECT id FROM diff_mv_refresh_txn ORDER BY id;
ROLLBACK;
SELECT id FROM diff_mv_refresh_txn ORDER BY id;
DROP MATERIALIZED VIEW diff_mv_refresh_txn;
DROP TABLE diff_mv_refresh_src;
