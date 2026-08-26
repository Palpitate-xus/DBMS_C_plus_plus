-- translate character mapping
DROP TABLE IF EXISTS diff_tr1
CREATE TABLE diff_tr1 (v text)
INSERT INTO diff_tr1 VALUES ('hello')
SELECT translate(v, 'el', 'ip') FROM diff_tr1
SELECT translate(v, 'el', '') FROM diff_tr1
SELECT translate(v, 'l', 'LXY') FROM diff_tr1
