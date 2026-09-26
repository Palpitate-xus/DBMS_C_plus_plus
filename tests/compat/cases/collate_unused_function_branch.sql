-- A conditional result inherits collation through an unused text-function branch.
SELECT COALESCE('apple', lower('X' COLLATE "C")) < 'Zoo' AS result;
SELECT CASE WHEN true THEN 'apple' ELSE replace('x' COLLATE "C", 'x', 'x') END < 'Zoo' AS result;
SELECT COALESCE('apple', replace('x' COLLATE "C", 'x' COLLATE "default", 'x')) AS result;
