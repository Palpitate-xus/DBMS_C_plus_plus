-- aggregate function semantics
DROP TABLE IF EXISTS diff_agg
CREATE TABLE diff_agg (g text, v numeric)
INSERT INTO diff_agg VALUES ('a', 10), ('a', 20), ('b', 5), ('b', NULL)
SELECT sum(v) FROM diff_agg
SELECT count(v) FROM diff_agg
SELECT count(*) FROM diff_agg
SELECT avg(v) FROM diff_agg
SELECT max(v), min(v) FROM diff_agg
SELECT g, sum(v) FROM diff_agg GROUP BY g ORDER BY g
SELECT g, count(*) FROM diff_agg GROUP BY g ORDER BY g
SELECT sum(v) FILTER (WHERE v > 5) FROM diff_agg
