DROP TABLE IF EXISTS diff_group_pk;
CREATE TABLE diff_group_pk (id int PRIMARY KEY, payload text);
INSERT INTO diff_group_pk VALUES (2, 'two words'), (1, 'one'), (3, NULL);
SELECT id, payload, count(*) FROM diff_group_pk GROUP BY id ORDER BY id;
SELECT payload, count(*) FROM diff_group_pk GROUP BY id ORDER BY id;
SELECT payload AS id, count(*) FROM diff_group_pk GROUP BY id ORDER BY id;
DROP TABLE diff_group_pk;
