DROP TABLE IF EXISTS diff_ctpart1;
DROP TABLE IF EXISTS diff_ctpart2;
DROP TABLE IF EXISTS diff_ctpart3;
CREATE TABLE diff_ctpart1 (id integer) PARTITION;
CREATE TABLE diff_ctpart2 (id integer) PARTITION BY RANGE;
SELECT id FROM diff_ctpart1;
SELECT id FROM diff_ctpart2;
CREATE TABLE diff_ctpart3 (id integer) PARTITION BY RANGE (id);
SELECT id FROM diff_ctpart3;
