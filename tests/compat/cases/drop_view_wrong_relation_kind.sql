DROP TABLE IF EXISTS diff_drop_view_table;
CREATE TABLE diff_drop_view_table (id integer);
INSERT INTO diff_drop_view_table VALUES (111);
DROP VIEW diff_drop_view_table;
DROP VIEW IF EXISTS diff_drop_view_table;
SELECT id FROM diff_drop_view_table;
DROP TABLE diff_drop_view_table;
