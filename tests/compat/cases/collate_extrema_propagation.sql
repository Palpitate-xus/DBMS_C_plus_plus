-- GREATEST and LEAST derive one collation from all arguments, including NULLs.
SELECT GREATEST('apple', 'A' COLLATE "C") < 'Zoo' AS result;
SELECT LEAST('apple', 'x' COLLATE "C") < 'Zoo' AS result;
SELECT GREATEST('apple' COLLATE "C", NULL::text COLLATE "default") AS result;
SELECT LEAST('apple' COLLATE "C", NULL::text COLLATE "default") AS result;
