-- DROP SCHEMA without IF EXISTS reports invalid_schema_name.
DROP SCHEMA IF EXISTS pgdiff_missing_schema;
DROP SCHEMA pgdiff_missing_schema;
SELECT 1;
