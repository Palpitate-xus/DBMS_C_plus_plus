-- Table scalar subqueries must not decode text from display rows.
CREATE TABLE diff_scalar_wire(id integer, v text);
INSERT INTO diff_scalar_wire VALUES (1, NULL), (2, ''), (3, 'NULL'), (4, 'a b');
SELECT (SELECT v FROM diff_scalar_wire WHERE id = 1) AS v;
SELECT (SELECT v FROM diff_scalar_wire WHERE id = 2) AS v;
SELECT (SELECT v FROM diff_scalar_wire WHERE id = 3) AS v;
SELECT (SELECT v FROM diff_scalar_wire WHERE id = 4) AS v;
SELECT (select v from diff_scalar_wire where id = 4) AS v;
SELECT (SELECT v FROM diff_scalar_wire WHERE id = 999) AS v;
SELECT (SELECT id FROM diff_scalar_wire WHERE id = 1) AS n;
SELECT (SELECT v FROM diff_scalar_wire) AS bad;
SELECT (SELECT v, id FROM diff_scalar_wire WHERE id = 1) AS bad;
DROP TABLE diff_scalar_wire;
