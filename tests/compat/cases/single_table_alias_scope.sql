DROP TABLE IF EXISTS diff_single_alias;
CREATE TABLE diff_single_alias (v int);
INSERT INTO diff_single_alias VALUES (1);
SELECT diff_single_alias.v FROM diff_single_alias AS x;
SELECT diff_single_alias.v FROM diff_single_alias x;
SELECT x.v FROM diff_single_alias AS x;
SELECT diff_single_alias.v FROM diff_single_alias;
SELECT other.v FROM diff_single_alias AS x;
DROP TABLE diff_single_alias;
