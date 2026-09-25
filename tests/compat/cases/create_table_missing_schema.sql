CREATE TABLE diff_create_table_missing_schema.t (id integer);
CREATE TABLE IF NOT EXISTS diff_create_table_missing_schema.t (id integer);
SET search_path TO diff_create_table_missing_schema;
CREATE TABLE t (id integer);
SET search_path TO public;
SELECT 1;
