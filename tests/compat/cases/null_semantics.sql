-- NULL semantics: literals, coalesce, nullif, greatest/least, FILTER
SELECT coalesce(NULL, NULL, 7)
SELECT nullif(5, 5), nullif(5, 6)
SELECT greatest(1, 3), least(4, 2)
SELECT NULL::text IS NULL
SELECT 1 + NULL
SELECT 'a' || NULL
SELECT NULL AND true, NULL OR true, NULL AND false, NULL OR false
SELECT NOT NULL
SELECT NULL IS NULL, NULL IS NOT NULL, 3 IS NULL
SELECT NULLIF(NULL, 1) IS NULL
