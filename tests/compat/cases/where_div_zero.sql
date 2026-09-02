DROP TABLE IF EXISTS diff_divzero;
CREATE TABLE diff_divzero (id INT, v INT);
INSERT INTO diff_divzero VALUES (1, 10), (2, 0);
SELECT id FROM diff_divzero WHERE v / 0 > 1;
SELECT id FROM diff_divzero WHERE v /0 > 1;
