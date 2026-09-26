-- COLLATE "default" denotes the database's locale, not C byte order.
SELECT v FROM (VALUES ('Zoo'), ('apple')) AS d(v) ORDER BY v COLLATE "default";
CREATE TEMP TABLE explicit_default_collation (v TEXT COLLATE "default");
INSERT INTO explicit_default_collation VALUES ('Zoo'), ('apple');
SELECT v FROM explicit_default_collation WHERE v < 'Zoo' ORDER BY v;
CREATE INDEX explicit_default_collation_v_idx ON explicit_default_collation(v);
SELECT v FROM explicit_default_collation WHERE v < 'Zoo' ORDER BY v;
