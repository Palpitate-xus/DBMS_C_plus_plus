-- Materialized views use undefined_table when absent.
DROP MATERIALIZED VIEW pgdiff_missing_matview;
SELECT 1;
