-- WITH DATA reports SELECT row count; WITH NO DATA reports CREATE MATERIALIZED VIEW.
DROP MATERIALIZED VIEW IF EXISTS pgdiff_matview_tags;
DROP MATERIALIZED VIEW IF EXISTS pgdiff_matview_nodata;
DROP TABLE IF EXISTS pgdiff_matview_source;
CREATE TABLE pgdiff_matview_source (a INT);
INSERT INTO pgdiff_matview_source VALUES (1);
CREATE MATERIALIZED VIEW pgdiff_matview_tags AS SELECT a FROM pgdiff_matview_source;
CREATE MATERIALIZED VIEW pgdiff_matview_nodata AS SELECT a FROM pgdiff_matview_source WITH NO DATA;
REFRESH MATERIALIZED VIEW pgdiff_matview_tags;
SELECT a FROM pgdiff_matview_tags;
