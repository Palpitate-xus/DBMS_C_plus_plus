-- Quoted multi-word aliases survive as single header cells
SELECT 1 AS "hello world";
SELECT 1 AS "a b", 2 AS "c d";
SELECT 'x' AS "with space";
SELECT 42 AS "the answer to everything";