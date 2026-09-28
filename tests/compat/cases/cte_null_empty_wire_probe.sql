-- CTE projection must preserve NULL, empty text, and literal NULL text.
WITH x AS (SELECT NULL::text AS v) SELECT v FROM x;
WITH x AS (SELECT ''::text AS v) SELECT v FROM x;
WITH x AS (SELECT 'NULL'::text AS v) SELECT v FROM x;
CREATE TABLE diff_cte_wire(id integer, v text);
INSERT INTO diff_cte_wire VALUES (1, NULL), (2, ''), (3, 'NULL'), (4, 'a b');
WITH x AS (SELECT id, v FROM diff_cte_wire) SELECT v FROM x ORDER BY id;
WITH x AS (SELECT ''::text AS v UNION ALL SELECT NULL::text AS v) SELECT v FROM x ORDER BY v NULLS FIRST;
DROP TABLE diff_cte_wire;
