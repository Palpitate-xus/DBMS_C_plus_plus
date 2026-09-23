-- Duplicate schemas use duplicate_schema, not an internal error.
DROP SCHEMA IF EXISTS pgdiff_duplicate_schema CASCADE;
CREATE SCHEMA pgdiff_duplicate_schema;
CREATE SCHEMA pgdiff_duplicate_schema;
SELECT 1;
