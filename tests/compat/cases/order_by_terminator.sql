CREATE TABLE diff_order_terminator(id INT);
INSERT INTO diff_order_terminator VALUES(1),(2),(3);
SELECT id AS n FROM diff_order_terminator ORDER BY n DESC;
SELECT id AS n FROM diff_order_terminator ORDER BY n DESC; /* trailing */
SELECT id AS n FROM diff_order_terminator ORDER BY n DESC; -- trailing
SELECT id AS n FROM diff_order_terminator ORDER BY n DESC; /* outer /* nested */ end */ -- trailing
SELECT id AS "n; label",'; /* literal */' AS data FROM diff_order_terminator ORDER BY id DESC; /* trailing */
SELECT id AS "n; label",E'escaped; -- text' AS data FROM diff_order_terminator ORDER BY id DESC; /* trailing */
SELECT id AS "n; label",$body$dollar; /* text */$body$ AS data FROM diff_order_terminator ORDER BY id DESC; -- trailing
SELECT id FROM diff_order_terminator ORDER BY id DESC,id ASC;
DROP TABLE diff_order_terminator;
