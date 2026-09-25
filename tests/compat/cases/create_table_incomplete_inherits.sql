DROP TABLE IF EXISTS diff_ctinherit;
CREATE TABLE diff_ctinherit (id integer) INHERITS;
CREATE TABLE diff_ctinherit (id integer) INHERITS ();
SELECT id FROM diff_ctinherit;
