-- join semantics
DROP TABLE IF EXISTS diff_j1
DROP TABLE IF EXISTS diff_j2
CREATE TABLE diff_j1 (id int, name text)
CREATE TABLE diff_j2 (id int, val numeric)
INSERT INTO diff_j1 VALUES (1, 'a'), (2, 'b'), (3, 'c')
INSERT INTO diff_j2 VALUES (1, 10), (2, 20), (4, 40)
SELECT * FROM diff_j1 INNER JOIN diff_j2 ON diff_j1.id = diff_j2.id ORDER BY diff_j1.id
SELECT * FROM diff_j1 LEFT JOIN diff_j2 ON diff_j1.id = diff_j2.id ORDER BY diff_j1.id
SELECT * FROM diff_j1 RIGHT JOIN diff_j2 ON diff_j1.id = diff_j2.id ORDER BY diff_j2.id
SELECT name, val FROM diff_j1 j JOIN diff_j2 ON j.id = diff_j2.id ORDER BY name
SELECT count(*) FROM diff_j1 CROSS JOIN diff_j2
