-- Wire NULL (-1) propagation through sorts and projections
DROP TABLE IF EXISTS diff_null_wire;
CREATE TABLE diff_null_wire (k INT, v TEXT);
INSERT INTO diff_null_wire VALUES (1, NULL), (2, 'x'), (3, 'y');
SELECT k, v FROM diff_null_wire;
SELECT k, v FROM diff_null_wire ORDER BY k;
SELECT v FROM diff_null_wire ORDER BY v;
SELECT k, v FROM diff_null_wire ORDER BY v DESC;