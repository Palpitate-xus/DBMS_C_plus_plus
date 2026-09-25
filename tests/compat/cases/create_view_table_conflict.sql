DROP TABLE IF EXISTS diff_view_table_conflict;
CREATE TABLE diff_view_table_conflict (id integer);
CREATE VIEW diff_view_table_conflict AS SELECT 1 AS id;
CREATE OR REPLACE VIEW diff_view_table_conflict AS SELECT 1 AS id;
INSERT INTO diff_view_table_conflict VALUES (101);
SELECT id FROM diff_view_table_conflict;
DROP TABLE diff_view_table_conflict;
