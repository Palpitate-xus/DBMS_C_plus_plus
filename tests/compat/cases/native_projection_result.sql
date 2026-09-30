-- Native semi/existence filters must preserve a literal NULL and empty text.
DROP TABLE IF EXISTS diff_native_result;
DROP TABLE IF EXISTS diff_native_result_keys;
CREATE TABLE diff_native_result(v NUMERIC,t TEXT,k INT);
CREATE TABLE diff_native_result_keys(k INT);
INSERT INTO diff_native_result_keys VALUES (1);
INSERT INTO diff_native_result VALUES (0.50,'same',1),(0.500,'same',1),(2.00,'',1),(NULL,'NULL',1),(NULL,NULL,1);
SELECT t FROM diff_native_result WHERE k IN (SELECT k FROM diff_native_result_keys) ORDER BY 1;
SELECT DISTINCT t FROM diff_native_result WHERE k IN (SELECT k FROM diff_native_result_keys) ORDER BY 1;
SELECT DISTINCT v,t FROM diff_native_result WHERE k IN (SELECT k FROM diff_native_result_keys) ORDER BY 1,2;
SELECT t FROM diff_native_result WHERE EXISTS (SELECT k FROM diff_native_result_keys) ORDER BY 1;
SELECT DISTINCT t FROM diff_native_result WHERE EXISTS (SELECT k FROM diff_native_result_keys) ORDER BY 1;
SELECT DISTINCT v,t FROM diff_native_result WHERE EXISTS (SELECT k FROM diff_native_result_keys) ORDER BY 1,2;
DROP TABLE diff_native_result;
DROP TABLE diff_native_result_keys;
