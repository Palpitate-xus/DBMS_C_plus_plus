-- Place the composed and decomposed spellings of é in the same table.
CREATE TABLE stored_text_filter (id integer PRIMARY KEY, label text, peer varchar(24), padded character(4));
INSERT INTO stored_text_filter VALUES (1, 'é', 'é', 'a'), (2, 'é', 'é', 'a '), (3, '', '', ''), (4, ' ', '', ' '), (5, NULL, NULL, NULL), (6, 'A', 'a', 'A'), (7, '01', '1', 'a');
SELECT id FROM stored_text_filter WHERE label = 'é' ORDER BY id;
SELECT id FROM stored_text_filter WHERE label <> 'é' ORDER BY id;
SELECT id FROM stored_text_filter WHERE label = '' ORDER BY id;
SELECT id FROM stored_text_filter WHERE label = peer ORDER BY id;
SELECT id FROM stored_text_filter WHERE label != peer ORDER BY id;
SELECT id FROM stored_text_filter WHERE padded = 'a ' ORDER BY id;
SELECT id FROM stored_text_filter WHERE padded <> 'a ' ORDER BY id;
SELECT id FROM stored_text_filter WHERE padded = '' ORDER BY id;
SELECT id FROM stored_text_filter WHERE label = NULL ORDER BY id;
SELECT id FROM stored_text_filter WHERE label IS NULL ORDER BY id;
SELECT id FROM stored_text_filter WHERE label COLLATE "C" < 'A' ORDER BY id;
SELECT id FROM stored_text_filter ORDER BY label COLLATE "C" NULLS LAST, id;
BEGIN;
UPDATE stored_text_filter SET label = 'changed' WHERE label = peer;
DELETE FROM stored_text_filter WHERE label = 'é';
SELECT id, label FROM stored_text_filter ORDER BY id;
ROLLBACK;
SELECT id, label FROM stored_text_filter ORDER BY id;

CREATE TABLE stored_text_parent (k text, padded character(4), part integer);
INSERT INTO stored_text_parent VALUES ('é', 'a', 1), ('é', 'a ', 2), ('', '', 1), (' ', ' ', 2), ('key-a', 'z', 1), ('key-b', 'Z', 1);
ALTER TABLE stored_text_parent ADD CONSTRAINT stored_text_parent_key PRIMARY KEY (k);
ALTER TABLE stored_text_parent ADD CONSTRAINT stored_padded_pair UNIQUE (padded, part);
CREATE UNIQUE INDEX stored_text_parent_pair ON stored_text_parent (k, part);
CREATE UNIQUE INDEX stored_text_parent_label ON stored_text_parent (k);
CREATE UNIQUE INDEX stored_padded_duplicate ON stored_text_parent (padded);
INSERT INTO stored_text_parent VALUES ('é', 'd', 4);
INSERT INTO stored_text_parent VALUES ('next', 'a ', 1);
INSERT INTO stored_text_parent VALUES ('key-c', NULL, 1);
SELECT k FROM stored_text_parent WHERE k = 'key-b';

CREATE TABLE stored_text_child (id integer PRIMARY KEY, parent_key text);
INSERT INTO stored_text_child VALUES (1,'é'), (2,'é'), (3,''), (4,' '), (5,NULL), (6,'missing');
ALTER TABLE stored_text_child ADD CONSTRAINT stored_child_parent FOREIGN KEY (parent_key) REFERENCES stored_text_parent(k) NOT VALID;
ALTER TABLE stored_text_child VALIDATE CONSTRAINT stored_child_parent;
UPDATE stored_text_child SET parent_key = 'key-a' WHERE id = 6;
ALTER TABLE stored_text_child VALIDATE CONSTRAINT stored_child_parent;
INSERT INTO stored_text_child VALUES (7,'E');
INSERT INTO stored_text_child VALUES (7,'key-b');
DELETE FROM stored_text_parent WHERE k = 'é';
SELECT id, parent_key FROM stored_text_child ORDER BY id;

-- The existing secondary index can contain distinct full keys with a common prefix.
CREATE TABLE stored_text_long_keys (k text, part integer);
INSERT INTO stored_text_long_keys VALUES ('prefixabcdefghijklmnopqrst-A', 1), ('prefixabcdefghijklmnopqrst-B', 1);
CREATE UNIQUE INDEX stored_text_long_unique ON stored_text_long_keys (k);
INSERT INTO stored_text_long_keys VALUES ('prefixabcdefghijklmnopqrst-A', 2);
SELECT k FROM stored_text_long_keys WHERE k = 'prefixabcdefghijklmnopqrst-B';
