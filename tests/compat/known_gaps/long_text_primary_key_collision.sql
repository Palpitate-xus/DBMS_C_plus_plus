-- Both values must remain findable after creating a primary key on existing rows.
CREATE TABLE long_text_primary (k text);
INSERT INTO long_text_primary VALUES ('prefixabcdefghijklmnopqrst-A'), ('prefixabcdefghijklmnopqrst-B');
ALTER TABLE long_text_primary ADD CONSTRAINT long_text_primary_key PRIMARY KEY (k);
SELECT k FROM long_text_primary ORDER BY k;
SELECT k FROM long_text_primary WHERE k = 'prefixabcdefghijklmnopqrst-B';
INSERT INTO long_text_primary VALUES ('prefixabcdefghijklmnopqrst-C');
CREATE TABLE long_text_reference (k text REFERENCES long_text_primary(k));
INSERT INTO long_text_reference VALUES ('prefixabcdefghijklmnopqrst-B');
