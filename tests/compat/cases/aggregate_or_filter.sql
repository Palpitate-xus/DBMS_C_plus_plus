DROP TABLE IF EXISTS diff_aof;
CREATE TABLE diff_aof (x integer);
INSERT INTO diff_aof VALUES (1), (2), (2), (NULL);
SELECT count(*) FROM diff_aof WHERE x = 1 OR x = 2;
SELECT sum(x) FROM diff_aof WHERE x = 1 OR x = 2;
SELECT count(*) FROM diff_aof WHERE x >= 1 OR x = 2;
SELECT count(*) FROM diff_aof WHERE x = 2 OR x = 2;
SELECT count(*) FROM diff_aof WHERE x = 3 OR x = 4;
SELECT sum(x) FROM diff_aof WHERE x = 3 OR x = 4;
