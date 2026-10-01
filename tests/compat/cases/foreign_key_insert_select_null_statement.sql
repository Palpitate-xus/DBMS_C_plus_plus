CREATE TABLE diff_fk_select_source(id INT,pid INT);
INSERT INTO diff_fk_select_source VALUES(1,2),(2,NULL);
CREATE TABLE diff_fk_select_target(id INT PRIMARY KEY,pid INT REFERENCES diff_fk_select_target(id));
INSERT INTO diff_fk_select_target SELECT id,pid FROM diff_fk_select_source RETURNING id,pid;
SELECT id,pid FROM diff_fk_select_target ORDER BY id;
DROP TABLE diff_fk_select_target,diff_fk_select_source;
