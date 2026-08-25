-- window functions
DROP TABLE IF EXISTS diff_w
CREATE TABLE diff_w (g text, v numeric)
INSERT INTO diff_w VALUES ('a', 1), ('a', 2), ('a', 2), ('b', 3)
SELECT g, v, row_number() over (partition by g order by v) AS rn FROM diff_w ORDER BY g, v
SELECT g, v, rank() over (partition by g order by v) AS rk FROM diff_w ORDER BY g, v
SELECT g, v, dense_rank() over (partition by g order by v) AS dr FROM diff_w ORDER BY g, v
SELECT g, v, sum(v) over (partition by g) AS sv FROM diff_w ORDER BY g, v
SELECT g, v, avg(v) over (partition by g) AS av FROM diff_w ORDER BY g, v
SELECT g, v, min(v) over (partition by g) AS mn FROM diff_w ORDER BY g, v
SELECT g, v, max(v) over (partition by g) AS mx FROM diff_w ORDER BY g, v
SELECT g, v, count(*) over (partition by g) AS cn FROM diff_w ORDER BY g, v
SELECT g, row_number() over (order by v) AS rn FROM diff_w ORDER BY g
