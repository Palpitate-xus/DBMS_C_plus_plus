-- Per-row projection of comparison and LIKE expressions over table columns
DROP TABLE IF EXISTS proj_expr;
CREATE TABLE proj_expr (t TEXT, n INT);
INSERT INTO proj_expr VALUES ('abc', 1), ('bcd', 2);
SELECT t LIKE 'a%' FROM proj_expr;
SELECT t NOT LIKE 'a%' FROM proj_expr;
SELECT t <> 'abc' FROM proj_expr;
SELECT t < 'b' FROM proj_expr;
SELECT t > 'b' FROM proj_expr;
SELECT t LIKE 'a%' AS is_a FROM proj_expr;
SELECT n > 1 FROM proj_expr;
SELECT n = 1 AND t LIKE 'a%' FROM proj_expr;