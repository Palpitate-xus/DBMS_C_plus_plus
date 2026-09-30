-- Force native execution with a function predicate beside an indexed key.
DROP TABLE IF EXISTS diff_index_recheck_secondary;
DROP TABLE IF EXISTS diff_index_recheck_primary;
CREATE TABLE diff_index_recheck_secondary(id INT PRIMARY KEY,k TEXT);
INSERT INTO diff_index_recheck_secondary VALUES (1,'12345678901234567890-alpha'),(2,'12345678901234567890-beta');
CREATE INDEX diff_index_recheck_k ON diff_index_recheck_secondary(k);
SELECT id,k FROM diff_index_recheck_secondary WHERE k='12345678901234567890-alpha' AND length(k)>0;
SELECT id,k FROM diff_index_recheck_secondary WHERE k='12345678901234567890-beta' AND length(k)>0;
SELECT id,k FROM diff_index_recheck_secondary WHERE k='12345678901234567890-missing' AND length(k)>0;
CREATE TABLE diff_index_recheck_primary(id INT,k TEXT PRIMARY KEY);
INSERT INTO diff_index_recheck_primary VALUES (1,'12345678901234567890-alpha');
SELECT id,k FROM diff_index_recheck_primary WHERE k='12345678901234567890-alpha' AND length(k)>0;
SELECT id,k FROM diff_index_recheck_primary WHERE k='12345678901234567890-missing' AND length(k)>0;
DROP TABLE diff_index_recheck_secondary;
DROP TABLE diff_index_recheck_primary;
