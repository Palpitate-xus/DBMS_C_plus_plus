-- OR REPLACE does not change CREATE VIEW's command tag.
DROP VIEW IF EXISTS pgdiff_replace_view_tag;
CREATE VIEW pgdiff_replace_view_tag AS SELECT 1 AS a;
CREATE OR REPLACE VIEW pgdiff_replace_view_tag AS SELECT 2 AS a;
SELECT a FROM pgdiff_replace_view_tag;
