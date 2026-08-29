-- Empty string vs NULL: three-valued logic and wire markers
DROP TABLE IF EXISTS diff_env;
CREATE TABLE diff_env (k INT, v TEXT);
INSERT INTO diff_env VALUES (1, NULL), (2, ''), (3, 'a');
SELECT k, v FROM diff_env ORDER BY k;
SELECT k FROM diff_env WHERE v = '';
SELECT k FROM diff_env WHERE v <> 'a';
SELECT k FROM diff_env WHERE v IS NULL;
SELECT k FROM diff_env WHERE v IS NOT NULL;
SELECT count(*), count(v) FROM diff_env;
SELECT coalesce(v, 'NIL') FROM diff_env ORDER BY 1;