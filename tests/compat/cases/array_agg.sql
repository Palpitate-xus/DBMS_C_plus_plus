DROP TABLE IF EXISTS diff_aagg134;
CREATE TABLE diff_aagg134 (g VARCHAR(4), v INT);
INSERT INTO diff_aagg134 VALUES ('a', 10), ('a', 20), ('b', 5), ('b', 15), ('b', 15);
SELECT array_agg(v) FROM diff_aagg134;
SELECT array_agg(DISTINCT v) FROM diff_aagg134;
SELECT array_agg(DISTINCT g) FROM diff_aagg134;
SELECT g, array_agg(v) FROM diff_aagg134 GROUP BY g ORDER BY g;
SELECT array_agg(v) FILTER (WHERE v > 10) FROM diff_aagg134;
