DROP TABLE IF EXISTS diff_exists;
CREATE TABLE diff_exists (id INT PRIMARY KEY, name VARCHAR(20));
INSERT INTO diff_exists VALUES (1, 'a'), (2, 'b');
SELECT EXISTS (SELECT 1 FROM diff_exists WHERE id = 1) AS e1;
SELECT EXISTS (SELECT 1 FROM diff_exists WHERE id = 9) AS e2;
SELECT NOT EXISTS (SELECT 1 FROM diff_exists WHERE id = 9) AS ne1;
SELECT NOT EXISTS (SELECT 1 FROM diff_exists WHERE id = 2) AS ne2;
SELECT EXISTS (SELECT 1 FROM diff_exists) AS e3;
SELECT EXISTS (SELECT 1 FROM diff_exists WHERE name = 'b') AS e4;
