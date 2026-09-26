-- Other scalar text functions must also sort by their evaluated results.
CREATE TEMP TABLE diff_order_other_text(v text);
INSERT INTO diff_order_other_text VALUES ('ab'), ('ba');
SELECT v FROM diff_order_other_text ORDER BY substring(v from 2 for 1) COLLATE "C";
SELECT v FROM diff_order_other_text ORDER BY overlay(v placing 'z' from 1 for 1) COLLATE "C";
SELECT v FROM diff_order_other_text ORDER BY trim(leading 'b' from v) COLLATE "C";
DROP TABLE diff_order_other_text;
