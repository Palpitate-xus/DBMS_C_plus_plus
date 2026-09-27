-- Cascading a source DROP must restore both source and dependent materialized view.
CREATE TABLE diff_mv_cascade_src(id integer);
INSERT INTO diff_mv_cascade_src VALUES (7);
CREATE MATERIALIZED VIEW diff_mv_cascade_txn AS SELECT id FROM diff_mv_cascade_src;
BEGIN;
DROP TABLE diff_mv_cascade_src CASCADE;
ROLLBACK;
SELECT id FROM diff_mv_cascade_src;
SELECT id FROM diff_mv_cascade_txn;
DROP MATERIALIZED VIEW diff_mv_cascade_txn;
DROP TABLE diff_mv_cascade_src;
