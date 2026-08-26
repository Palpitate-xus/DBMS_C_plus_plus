-- multiple correlation predicates in EXISTS / NOT EXISTS
DROP TABLE IF EXISTS diff_mc1
DROP TABLE IF EXISTS diff_mc2
CREATE TABLE diff_mc1 (a numeric, b numeric)
CREATE TABLE diff_mc2 (a numeric, b numeric, tag text)
INSERT INTO diff_mc1 VALUES (1, 10), (2, 20)
INSERT INTO diff_mc2 VALUES (1, 10, 'cat'), (1, 30, 'dog'), (2, 20, 'cat'), (2, 99, 'emu')
SELECT a, b FROM diff_mc1 WHERE EXISTS (SELECT 1 FROM diff_mc2 WHERE diff_mc2.a = diff_mc1.a AND diff_mc2.b = diff_mc1.b) ORDER BY a
SELECT a FROM diff_mc1 WHERE EXISTS (SELECT 1 FROM diff_mc2 WHERE diff_mc2.a = diff_mc1.a AND diff_mc2.b > diff_mc1.b) ORDER BY a
SELECT a, b FROM diff_mc1 WHERE NOT EXISTS (SELECT 1 FROM diff_mc2 WHERE diff_mc2.a = diff_mc1.a AND diff_mc2.b = diff_mc1.b) ORDER BY a
