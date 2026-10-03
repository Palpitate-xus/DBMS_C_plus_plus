-- Composite row equality must distinguish FALSE from UNKNOWN per inner row.
DROP TABLE IF EXISTS diff_composite_not_in_outer
DROP TABLE IF EXISTS diff_composite_not_in_inner
CREATE TABLE diff_composite_not_in_outer (a INT, b INT)
INSERT INTO diff_composite_not_in_outer VALUES (1, 2), (4, 5), (3, NULL), (NULL, 3)
CREATE TABLE diff_composite_not_in_inner (a INT, b INT)
INSERT INTO diff_composite_not_in_inner VALUES (3, NULL)
SELECT a, b FROM diff_composite_not_in_outer WHERE (a, b) NOT IN (SELECT a, b FROM diff_composite_not_in_inner) ORDER BY a
SELECT a, b FROM diff_composite_not_in_outer WHERE (a, b) IN (SELECT a, b FROM diff_composite_not_in_inner) ORDER BY a
INSERT INTO diff_composite_not_in_inner VALUES (1, 2)
SELECT a, b FROM diff_composite_not_in_outer WHERE (a, b) IN (SELECT a, b FROM diff_composite_not_in_inner) ORDER BY a
SELECT a, b FROM diff_composite_not_in_outer WHERE (a, b) NOT IN (SELECT a, b FROM diff_composite_not_in_inner) ORDER BY a
DELETE FROM diff_composite_not_in_inner
SELECT a, b FROM diff_composite_not_in_outer WHERE (a, b) IN (SELECT a, b FROM diff_composite_not_in_inner) ORDER BY a
SELECT a, b FROM diff_composite_not_in_outer WHERE (a, b) NOT IN (SELECT a, b FROM diff_composite_not_in_inner) ORDER BY a
