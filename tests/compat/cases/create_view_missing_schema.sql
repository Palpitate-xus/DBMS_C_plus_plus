DROP TABLE IF EXISTS diff_create_view_source;
CREATE TABLE diff_create_view_source (id integer);
CREATE VIEW diff_create_view_missing.v AS SELECT id FROM diff_create_view_source;
DROP TABLE diff_create_view_source;
