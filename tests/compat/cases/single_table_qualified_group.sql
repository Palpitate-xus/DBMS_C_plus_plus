DROP TABLE IF EXISTS diff_qualified_group;
CREATE TABLE diff_qualified_group (v int);
INSERT INTO diff_qualified_group VALUES (-1), (1), (2);
SELECT x.v, count(*) FROM diff_qualified_group AS x GROUP BY x.v ORDER BY x.v;
SELECT x.v, count(*) FROM diff_qualified_group AS x GROUP BY x.v HAVING x.v > 0 ORDER BY x.v;
SELECT diff_qualified_group.v, count(*) FROM diff_qualified_group GROUP BY diff_qualified_group.v ORDER BY diff_qualified_group.v;
DROP TABLE diff_qualified_group;
