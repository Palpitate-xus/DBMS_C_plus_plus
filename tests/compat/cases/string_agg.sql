DROP TABLE IF EXISTS diff_sagg133;
CREATE TABLE diff_sagg133 (g VARCHAR(4), v INT);
INSERT INTO diff_sagg133 VALUES ('a', 10), ('a', 20), ('b', 5), ('b', 15), ('b', 15);
SELECT string_agg(g, ',') FROM diff_sagg133;
SELECT string_agg(DISTINCT g, ',') FROM diff_sagg133;
SELECT string_agg(g, ',' ORDER BY g) FROM diff_sagg133;
SELECT g, string_agg(g, ',') FROM diff_sagg133 GROUP BY g ORDER BY g;
