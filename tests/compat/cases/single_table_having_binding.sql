DROP TABLE IF EXISTS diff_having_bind;
CREATE TABLE diff_having_bind (v int);
INSERT INTO diff_having_bind VALUES (1), (2);
SELECT v, count(*) AS cnt FROM diff_having_bind GROUP BY v HAVING cnt > 0 ORDER BY v;
SELECT v, count(*) FROM diff_having_bind GROUP BY v HAVING missing_column > 0 ORDER BY v;
SELECT x.v, count(*) FROM diff_having_bind AS x GROUP BY x.v HAVING diff_having_bind.v > 0 ORDER BY x.v;
SELECT v, count(*) FROM diff_having_bind GROUP BY v HAVING count(*) > 0 ORDER BY v;
DROP TABLE diff_having_bind;
