DROP TABLE IF EXISTS diff_exproj;
CREATE TABLE diff_exproj (id INT, v INT);
INSERT INTO diff_exproj VALUES (1, 10), (2, 21);
SELECT EXISTS (SELECT 1 FROM diff_exproj WHERE v > 15) FROM diff_exproj;
SELECT EXISTS (SELECT 1 FROM diff_exproj WHERE v > 15) AS has_big FROM diff_exproj;
SELECT id, EXISTS (SELECT 1 FROM diff_exproj WHERE v > 15) FROM diff_exproj;
SELECT EXISTS (SELECT 1 FROM diff_exproj WHERE v > 99) FROM diff_exproj;
SELECT NOT EXISTS (SELECT 1 FROM diff_exproj WHERE v > 15) FROM diff_exproj;
