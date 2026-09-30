DROP TABLE IF EXISTS diff_empty_index_values;
CREATE TABLE diff_empty_index_values(id INT PRIMARY KEY,k TEXT);
INSERT INTO diff_empty_index_values VALUES (1,''),(2,NULL),(3,'x');
CREATE INDEX diff_empty_index_k ON diff_empty_index_values(k);
INSERT INTO diff_empty_index_values VALUES (4,'');
SELECT id,k FROM diff_empty_index_values WHERE k='' AND length(k)=0 ORDER BY id;
SELECT id,k FROM diff_empty_index_values WHERE k='' AND id=1 AND length(k)=0;
SELECT id,k FROM diff_empty_index_values WHERE (k='' OR id=3) AND length(k)>=0 ORDER BY id;
DROP TABLE diff_empty_index_values;
