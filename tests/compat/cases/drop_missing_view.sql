-- Views occupy the relation namespace and use undefined_table when absent.
DROP VIEW IF EXISTS pgdiff_missing_view;
DROP VIEW pgdiff_missing_view;
SELECT 1;
