DROP TABLE IF EXISTS diff_substr;
CREATE TABLE diff_substr (v VARCHAR(20));
INSERT INTO diff_substr VALUES ('apple'), ('banana'), ('cherry'), (NULL);
SELECT substring(v from 2 for 3) FROM diff_substr;
SELECT substring(v from 2) FROM diff_substr;
SELECT substring(v, 2, 3) FROM diff_substr;
SELECT substr(v, 2, 3) FROM diff_substr;
SELECT substr(v, 2) FROM diff_substr;
