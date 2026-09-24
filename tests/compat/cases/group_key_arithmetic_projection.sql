DROP TABLE IF EXISTS diff_group_key_arith;
CREATE TABLE diff_group_key_arith (a int);
INSERT INTO diff_group_key_arith VALUES (1), (2), (NULL);
SELECT a, count(*) FROM diff_group_key_arith GROUP BY a ORDER BY a;
SELECT a + 1, count(*) FROM diff_group_key_arith GROUP BY a ORDER BY a;
SELECT count(*), a + 1 AS next_a FROM diff_group_key_arith GROUP BY a ORDER BY a;
DROP TABLE diff_group_key_arith;
