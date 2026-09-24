DROP TABLE IF EXISTS diff_group_name_precedence;
CREATE TABLE diff_group_name_precedence (a int, b int);
INSERT INTO diff_group_name_precedence VALUES (1, 10), (1, 20);
SELECT a AS b, count(*) FROM diff_group_name_precedence GROUP BY b;
SELECT a AS c, count(*) FROM diff_group_name_precedence GROUP BY c ORDER BY c;
SELECT b, count(*) FROM diff_group_name_precedence GROUP BY b ORDER BY b;
DROP TABLE diff_group_name_precedence;
