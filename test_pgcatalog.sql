-- Test pg_catalog virtual tables

USE DATABASE testdb;

SELECT * FROM pg_catalog.pg_class;
SELECT * FROM pg_catalog.pg_attribute WHERE attrelid = 'employees';
SELECT * FROM pg_catalog.pg_type;
SELECT * FROM pg_catalog.pg_index WHERE indrelid = 'employees';
SELECT * FROM pg_catalog.pg_namespace;

-- Test filtering
SELECT * FROM pg_catalog.pg_class WHERE relname = 'employees';
SELECT * FROM pg_catalog.pg_attribute WHERE attrelid = 'employees' AND attname = 'id';
