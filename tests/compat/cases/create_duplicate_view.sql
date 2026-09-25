DROP VIEW IF EXISTS diff_duplicate_view;
DROP TABLE IF EXISTS diff_duplicate_view_source;
CREATE TABLE diff_duplicate_view_source (id integer);
INSERT INTO diff_duplicate_view_source VALUES (91);
CREATE VIEW diff_duplicate_view AS SELECT id FROM diff_duplicate_view_source;
CREATE VIEW diff_duplicate_view AS SELECT id + 1 AS id FROM diff_duplicate_view_source;
SELECT id FROM diff_duplicate_view;
DROP VIEW diff_duplicate_view;
