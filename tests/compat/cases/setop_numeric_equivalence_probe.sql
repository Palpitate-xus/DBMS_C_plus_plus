-- DISTINCT and multiset operations compare numeric value, not text scale.
SELECT 1.0::numeric AS v UNION SELECT 1.00::numeric AS v;
SELECT 1.0::numeric AS v INTERSECT SELECT 1.00::numeric AS v;
SELECT 1.0::numeric AS v EXCEPT SELECT 1.00::numeric AS v;
SELECT 1.0::numeric AS v UNION ALL SELECT 1.00::numeric AS v;
