-- CASE default result names can inherit the ELSE column reference.
DROP TABLE IF EXISTS pgdiff_table_case_headers;
CREATE TABLE pgdiff_table_case_headers (id INT);
INSERT INTO pgdiff_table_case_headers VALUES (7);
SELECT CASE WHEN id > 0 THEN 1 ELSE id END, CASE WHEN id > 0 THEN id END, CASE WHEN id > 0 THEN 'x' ELSE CAST(id AS text) END FROM pgdiff_table_case_headers;
