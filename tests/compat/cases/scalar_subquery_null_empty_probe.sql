-- Scalar-subquery projection must preserve distinct wire values and types.
SELECT (SELECT NULL::text) AS v;
SELECT (SELECT ''::text) AS v;
SELECT (SELECT 'NULL'::text) AS v;
SELECT (SELECT 42::integer) AS n;
SELECT (SELECT 42::integer WHERE false) AS n;
SELECT (SELECT 1, 2) AS bad;
SELECT (SELECT 1 UNION ALL SELECT 2) AS bad;
SELECT 7 AS id, (SELECT ''::text) AS v, 'NULL' AS literal;
