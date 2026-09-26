-- Public quoted dotted names and schema-qualified names must not share files.
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
DROP SEQUENCE IF EXISTS public."public.legacy_prefix_seq";
DROP SEQUENCE IF EXISTS public.legacy_prefix_seq;
CREATE SEQUENCE public."public.legacy_prefix_seq" START 31;
CREATE SEQUENCE public.legacy_prefix_seq START 41;
SELECT nextval('public."public.legacy_prefix_seq"');
SELECT nextval('public.legacy_prefix_seq');
DROP SEQUENCE public.legacy_prefix_seq;
DROP SEQUENCE public."public.legacy_prefix_seq";
