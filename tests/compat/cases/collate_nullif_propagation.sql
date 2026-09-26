-- NULLIF uses the combined collation of its two arguments.
SELECT NULLIF('apple', 'x' COLLATE "C") < 'Zoo' AS result;
SELECT NULLIF('apple' COLLATE "C", NULL::text COLLATE "default") AS result;
