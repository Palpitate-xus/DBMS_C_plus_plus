DROP TABLE IF EXISTS diff_create_ine;
CREATE TABLE diff_create_ine (id integer);
INSERT INTO diff_create_ine VALUES (131);
CREATE TABLE IF NOT EXISTS diff_create_ine (id text);
SELECT id FROM diff_create_ine;
DROP TABLE diff_create_ine;
