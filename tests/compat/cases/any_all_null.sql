-- ANY/ALL over ARRAY literals and IN-list NULL three-valued logic
DROP TABLE IF EXISTS diff_qa;
CREATE TABLE diff_qa (k INT);
INSERT INTO diff_qa VALUES (1), (2), (3), (5);
SELECT k FROM diff_qa WHERE k = ANY (ARRAY[1, 3]) ORDER BY k;
SELECT k FROM diff_qa WHERE k NOT IN (NULL, 3) ORDER BY k;
SELECT k FROM diff_qa WHERE k IN (NULL, 2) ORDER BY k;
SELECT k FROM diff_qa WHERE k NOT IN (3, 4) ORDER BY k;
