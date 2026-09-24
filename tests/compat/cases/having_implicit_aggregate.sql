DROP TABLE IF EXISTS diff_having_implicit;
CREATE TABLE diff_having_implicit (v int);
INSERT INTO diff_having_implicit VALUES (1), (2);
SELECT count(*) FROM diff_having_implicit HAVING count(*) > 1;
SELECT count(*) FROM diff_having_implicit HAVING count(*) > 2;
DELETE FROM diff_having_implicit;
SELECT count(*) FROM diff_having_implicit HAVING count(*) = 0;
SELECT count(*) FROM diff_having_implicit HAVING count(*) > 0;
DROP TABLE diff_having_implicit;
