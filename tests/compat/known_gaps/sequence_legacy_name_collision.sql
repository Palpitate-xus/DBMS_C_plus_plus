-- Legacy storage joins schema and relation with one dot.  A public quoted
-- relation containing a dot can therefore alias a schema-qualified relation.
DROP SCHEMA IF EXISTS diff_legacy_collision CASCADE;
DROP SEQUENCE IF EXISTS public."diff_legacy_collision.seq" CASCADE;
CREATE SCHEMA diff_legacy_collision;
CREATE SEQUENCE public."diff_legacy_collision.seq" START 7;
CREATE SEQUENCE diff_legacy_collision.seq START 11;
SELECT nextval('public."diff_legacy_collision.seq"');
SELECT nextval('diff_legacy_collision.seq');
DROP SEQUENCE diff_legacy_collision.seq;
DROP SEQUENCE public."diff_legacy_collision.seq";
DROP SCHEMA diff_legacy_collision;
