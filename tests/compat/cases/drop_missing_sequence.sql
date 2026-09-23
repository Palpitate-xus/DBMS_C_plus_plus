-- Sequences occupy the relation namespace and use undefined_table when absent.
DROP SEQUENCE IF EXISTS pgdiff_missing_sequence;
DROP SEQUENCE pgdiff_missing_sequence;
SELECT 1;
