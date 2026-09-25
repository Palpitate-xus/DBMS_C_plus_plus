DROP TABLE IF EXISTS diff_ujoin_a;
DROP TABLE IF EXISTS diff_ujoin_b;
CREATE TABLE diff_ujoin_a (id integer);
CREATE TABLE diff_ujoin_b (id integer);
INSERT INTO diff_ujoin_a VALUES (1);
INSERT INTO diff_ujoin_b VALUES (1);
SELECT a.id FROM diff_ujoin_a a JOIN diff_ujoin_b b ON a.id = b.id;
SELECT a.id FROM DIFF_UJOIN_A a JOIN DIFF_UJOIN_B b ON a.id = b.id;
