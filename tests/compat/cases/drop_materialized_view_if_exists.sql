-- A successful no-op DROP retains the full three-word command tag.
DROP MATERIALIZED VIEW IF EXISTS pgdiff_missing_matview;
SELECT 1;
