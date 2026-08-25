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
SELECT g, v, lag(v) over (order by v) AS lg FROM diff_w ORDER BY v
SELECT g, v, lead(v) over (order by v) AS ld FROM diff_w ORDER BY v
SELECT g, v, first_value(v) over (partition by g order by v) AS fv FROM diff_w ORDER BY v
SELECT g, v, last_value(v) over (partition by g order by v) AS lv FROM diff_w ORDER BY v
SELECT g, v, row_number() over w AS rn FROM diff_w WINDOW w AS (partition by g order by v) ORDER BY g, v
SELECT g, v, sum(v) over (order by v rows between 1 preceding and current row) AS rp FROM diff_w ORDER BY v
SELECT g, v, sum(v) over (order by v rows between current row and 1 following) AS rf FROM diff_w ORDER BY v
SELECT g, v, sum(v) over (order by v rows between unbounded preceding and unbounded following) AS ru FROM diff_w ORDER BY v
SELECT g, sum(v) over (partition by g rows between unbounded preceding and unbounded following) AS pu FROM diff_w ORDER BY g
SELECT g, v, sum(v) over (order by v range between 1 preceding and 1 following) AS rr FROM diff_w ORDER BY v
SELECT g, v, lag(v, 2) over (order by v) AS l2 FROM diff_w ORDER BY v
SELECT g, v, ntile(2) over (order by v) AS nt FROM diff_w ORDER BY v
SELECT g, v, sum(v) over (order by v groups between 1 preceding and current row) AS gr FROM diff_w ORDER BY v
SELECT g, v, avg(v) over (order by v range between unbounded preceding and current row) AS ar FROM diff_w ORDER BY v
SELECT g, v, sum(v) over (order by v rows between 1 preceding and 1 following exclude current row) AS ec FROM diff_w ORDER BY v
SELECT g, v, sum(v) over (order by v rows between 1 preceding and 1 following exclude group) AS eg FROM diff_w ORDER BY v
SELECT g, v, sum(v) over (order by v rows between 1 preceding and 1 following exclude ties) AS et FROM diff_w ORDER BY v
