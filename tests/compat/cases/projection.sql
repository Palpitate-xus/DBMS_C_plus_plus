-- projection order (PG projects columns in SELECT-list order)
DROP TABLE IF EXISTS diff_pr
CREATE TABLE diff_pr (a numeric, b text, c numeric)
INSERT INTO diff_pr VALUES (1, 'x', 2), (3, 'y', 4)
SELECT b, a FROM diff_pr ORDER BY a
SELECT c, b, a FROM diff_pr ORDER BY a
SELECT a AS aa, c AS cc FROM diff_pr ORDER BY aa
SELECT b AS first, a AS second FROM diff_pr ORDER BY second
SELECT a, b, a FROM diff_pr ORDER BY a
SELECT b, b FROM diff_pr ORDER BY a
SELECT a AS x, b, a AS y FROM diff_pr ORDER BY a
