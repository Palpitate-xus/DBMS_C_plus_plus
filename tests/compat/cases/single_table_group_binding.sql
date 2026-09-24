DROP TABLE IF EXISTS diff_group_bind;
CREATE TABLE diff_group_bind (v int);
INSERT INTO diff_group_bind VALUES (1), (2);
SELECT count(*) FROM diff_group_bind GROUP BY missing_column;
SELECT count(*) FROM diff_group_bind AS x GROUP BY x.missing_column;
SELECT count(*) FROM diff_group_bind AS x GROUP BY diff_group_bind.v;
SELECT count(*) FROM diff_group_bind GROUP BY v ORDER BY v;
DROP TABLE diff_group_bind;
