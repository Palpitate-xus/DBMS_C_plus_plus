-- Omitted columns evaluate defaults and preserve SQL NULL; NOT NULL is not synthesized.
DROP TABLE IF EXISTS insert_default_values_probe;
CREATE TABLE insert_default_values_probe (id INTEGER DEFAULT 42, ts TIMESTAMP DEFAULT '2020-01-01 00:00:00', note TEXT);
INSERT INTO insert_default_values_probe DEFAULT VALUES;
SELECT id, ts, note FROM insert_default_values_probe;
CREATE TABLE insert_default_values_required (id INTEGER NOT NULL);
INSERT INTO insert_default_values_required DEFAULT VALUES;
SELECT COUNT(*) FROM insert_default_values_required;
DROP TABLE insert_default_values_required;
DROP TABLE insert_default_values_probe;
