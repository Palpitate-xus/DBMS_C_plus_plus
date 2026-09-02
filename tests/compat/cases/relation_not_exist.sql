DROP TABLE IF EXISTS diff_cte_err;
CREATE TABLE diff_cte_err (id INT, parent INT);
INSERT INTO diff_cte_err VALUES (1, NULL), (2, 1), (3, 1);
WITH t AS (SELECT id FROM diff_cte_err UNION ALL SELECT id + 1 FROM t WHERE id < 3) SELECT id FROM t;
WITH t AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT n FROM t;
SELECT * FROM nosuch_tbl_152;
DROP TABLE nosuchdrop152;
