-- The set-operation tail must reject negative LIMIT, not wrap it to size_t.
SELECT 1 AS v UNION ALL SELECT 2 AS v LIMIT -1;
SELECT 1 AS v INTERSECT SELECT 1 AS v LIMIT -1;
SELECT 1 AS v EXCEPT SELECT 2 AS v LIMIT -1;
SELECT 1 AS v UNION ALL SELECT 2 AS v LIMIT -0;
