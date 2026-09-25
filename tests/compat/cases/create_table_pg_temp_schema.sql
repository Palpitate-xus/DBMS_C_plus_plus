CREATE TABLE pg_temp.diff_explicit_temp_schema (id integer);
INSERT INTO pg_temp.diff_explicit_temp_schema VALUES (3);
SELECT id FROM pg_temp.diff_explicit_temp_schema;
DROP TABLE pg_temp.diff_explicit_temp_schema;
CREATE TABLE pg_temp.diff_explicit_temp_schema (id integer);
DROP TABLE pg_temp.diff_explicit_temp_schema;
SELECT 1;
