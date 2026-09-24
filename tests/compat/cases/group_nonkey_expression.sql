DROP TABLE IF EXISTS diff_group_expr_bind;
CREATE TABLE diff_group_expr_bind (a int, b int);
INSERT INTO diff_group_expr_bind VALUES (1, 10), (2, 20);
SELECT a + 1, count(*) FROM diff_group_expr_bind GROUP BY b ORDER BY b;
SELECT a + b, count(*) FROM diff_group_expr_bind GROUP BY a + b ORDER BY a + b;
DROP TABLE diff_group_expr_bind;
