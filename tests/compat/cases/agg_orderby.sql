DROP TABLE IF EXISTS diff_agg132;
CREATE TABLE diff_agg132 (g VARCHAR(4), v INT);
INSERT INTO diff_agg132 VALUES ('a', 10), ('a', 20), ('b', 5), ('b', 15), ('b', 15);
SELECT count(DISTINCT v) FROM diff_agg132;
SELECT sum(v ORDER BY v) FROM diff_agg132;
SELECT count(*) FILTER (WHERE g = 'a') FROM diff_agg132;
SELECT bool_and(v > 0), bool_or(v > 100) FROM diff_agg132;
SELECT g, count(v) FROM diff_agg132 GROUP BY g HAVING count(v) > 1 ORDER BY g;
