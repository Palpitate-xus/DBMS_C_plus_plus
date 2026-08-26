-- strpos and overlay
DROP TABLE IF EXISTS diff_so1
CREATE TABLE diff_so1 (v text)
INSERT INTO diff_so1 VALUES ('hello')
SELECT strpos(v, 'l'), strpos(v, 'z') FROM diff_so1
SELECT overlay(v placing 'xx' from 2 for 3) FROM diff_so1
SELECT overlay(v placing 'XX' from 2) FROM diff_so1
