SELECT 'open
SELECT "open
SELECT $$open
SELECT $tag$open$other$
BEGIN;
SELECT E'open
SELECT 1;
ROLLBACK;
BEGIN;
SAVEPOINT child;
SELECT 'doubled''
ROLLBACK TO child;
SELECT 2;
ROLLBACK;
SELECT 'slash\'; /* open
SELECT 'slash\' AS v;
SELECT 'it''s' AS v;
SELECT E'it\'s';
SELECT E'quote\'';
SELECT $tag$/* data ' " */$tag$ AS v;
SELECT 1 AS foo$tag$bar$tag$;
