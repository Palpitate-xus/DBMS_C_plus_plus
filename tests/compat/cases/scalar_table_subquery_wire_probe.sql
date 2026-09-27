-- Table scalar subqueries must not decode text from display rows.
CREATE TABLE diff_scalar_wire(id integer, v text);
INSERT INTO diff_scalar_wire VALUES (1, NULL), (2, ''), (3, 'NULL'), (4, 'a b');
SELECT (SELECT v FROM diff_scalar_wire WHERE id = 1) AS v;
SELECT (SELECT v FROM diff_scalar_wire WHERE id = 2) AS v;
SELECT (SELECT v FROM diff_scalar_wire WHERE id = 3) AS v;
SELECT (SELECT v FROM diff_scalar_wire WHERE id = 4) AS v;
DROP TABLE diff_scalar_wire;
