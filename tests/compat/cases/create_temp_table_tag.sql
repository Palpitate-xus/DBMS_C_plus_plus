-- TEMP/TEMPORARY modifies a table, not its command tag.
CREATE TEMP TABLE pgdiff_temp_tag (a INT);
CREATE TEMPORARY TABLE pgdiff_temporary_tag (b INT);
INSERT INTO pgdiff_temp_tag VALUES (1);
SELECT a FROM pgdiff_temp_tag;
