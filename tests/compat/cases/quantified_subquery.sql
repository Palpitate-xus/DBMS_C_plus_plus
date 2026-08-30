-- Quantified comparisons over unnest(ARRAY[...]) subqueries
DROP TABLE IF EXISTS diff_qf;
CREATE TABLE diff_qf (k INT);
INSERT INTO diff_qf VALUES (1), (2), (3);
SELECT k FROM diff_qf WHERE k <> ALL (SELECT unnest(ARRAY[1,2])) ORDER BY k;
SELECT k FROM diff_qf WHERE k = ANY (SELECT unnest(ARRAY[NULL,2])) ORDER BY k;
SELECT k FROM diff_qf WHERE k <> ALL (SELECT unnest(ARRAY[NULL,1])) ORDER BY k;
SELECT k FROM diff_qf WHERE k > ANY (SELECT k FROM diff_qf WHERE k > 1) ORDER BY k;
SELECT k FROM diff_qf WHERE k <> ALL (SELECT k FROM diff_qf WHERE k < 3) ORDER BY k;
