DROP TABLE IF EXISTS diff_oc;
DROP TABLE IF EXISTS diff_ic;
CREATE TABLE diff_oc (id INT, v INT);
CREATE TABLE diff_ic (x INT);
INSERT INTO diff_oc VALUES (1, 10), (2, 21);
INSERT INTO diff_ic VALUES (1), (3);
SELECT id, EXISTS (SELECT 1 FROM diff_ic WHERE diff_ic.x = diff_oc.id) FROM diff_oc ORDER BY id;
SELECT id, EXISTS (SELECT 1 FROM diff_ic WHERE x = v) FROM diff_oc ORDER BY id;
SELECT id, EXISTS (SELECT 1 FROM diff_ic WHERE diff_ic.x = diff_oc.v) FROM diff_oc ORDER BY id;
