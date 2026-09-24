DROP TABLE IF EXISTS diff_qec;
CREATE TABLE diff_qec (id integer PRIMARY KEY, "A""B" text);
INSERT INTO diff_qec (id, "A""B") VALUES (1, 'value');
SELECT "A""B" FROM diff_qec;
SELECT id FROM diff_qec WHERE "A""B" = 'value';
