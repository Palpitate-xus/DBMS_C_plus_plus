-- The result collation of conditional text expressions is derived from all branches.
SELECT COALESCE('apple' COLLATE "C", 'x') < 'Zoo' AS result;
SELECT COALESCE('apple', 'x' COLLATE "C") < 'Zoo' AS result;
SELECT CASE WHEN true THEN 'apple' ELSE 'x' COLLATE "C" END < 'Zoo' AS result;
SELECT COALESCE('apple' COLLATE "C", 'x' COLLATE "default") < 'Zoo' AS result;
SELECT CASE WHEN true THEN 'apple' COLLATE "C" ELSE 'x' COLLATE "default" END < 'Zoo' AS result;
SELECT COALESCE('apple' COLLATE "C", 'x' COLLATE "default") AS result;
SELECT CASE WHEN true THEN 'apple' COLLATE "C" ELSE 'x' COLLATE "default" END AS result;
SELECT COALESCE('apple', 'x' COLLATE "missing_collation") AS result;
