-- GROUPING: bitmask over columns absent from the grouping set
DROP TABLE IF EXISTS diff_gs2;
CREATE TABLE diff_gs2 (a TEXT, b TEXT, v INT);
INSERT INTO diff_gs2 VALUES ('x', 'p', 1), ('x', 'q', 2), ('y', 'p', 3);
SELECT a, b, grouping(a, b), sum(v) FROM diff_gs2 GROUP BY GROUPING SETS ((a, b), (a), ()) ORDER BY a, b;
SELECT a, grouping(a), sum(v) FROM diff_gs2 GROUP BY ROLLUP (a) ORDER BY a NULLS LAST;
SELECT a, b, grouping(a, b) FROM diff_gs2 GROUP BY CUBE (a, b) ORDER BY a, b;
