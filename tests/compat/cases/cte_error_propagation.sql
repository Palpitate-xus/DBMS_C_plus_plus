-- A failing CTE body must abort once and report its original SQLSTATE.
WITH c AS (SELECT 1 / 0 AS v) SELECT v FROM c;
SELECT 2;
SELECT 3;
DROP TABLE IF EXISTS pgdiff_cte_error_target;
CREATE TABLE pgdiff_cte_error_target (id INT PRIMARY KEY);
INSERT INTO pgdiff_cte_error_target VALUES (1);
WITH c AS (INSERT INTO pgdiff_cte_error_target VALUES (1) RETURNING id) SELECT id FROM c;
SELECT id FROM pgdiff_cte_error_target;
DROP TABLE IF EXISTS pgdiff_cte_shadow_source;
CREATE TABLE pgdiff_cte_shadow_source (id INT);
INSERT INTO pgdiff_cte_shadow_source VALUES (7);
WITH pgdiff_cte_shadow_source AS (SELECT id FROM pgdiff_cte_shadow_source) SELECT id FROM pgdiff_cte_shadow_source;
