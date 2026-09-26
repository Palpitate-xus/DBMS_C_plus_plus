-- Unqualified ORDER BY on a derived table and CTE uses the database's
-- en_US.utf8 default, while an explicit C collation remains byte ordered.
SELECT v FROM (VALUES (''), (NULL), ('NULL'), ('a b')) AS d(v) ORDER BY v NULLS LAST;
WITH c(v) AS (VALUES (''), (NULL), ('NULL'), ('a b')) SELECT v FROM c ORDER BY v NULLS LAST;
SELECT v FROM (VALUES (''), (NULL), ('NULL'), ('a b')) AS d(v) ORDER BY v COLLATE "C" NULLS LAST;
WITH c(v) AS (VALUES (''), (NULL), ('NULL'), ('a b')) SELECT v FROM c WHERE v IS NULL;
WITH c(v) AS (VALUES (''), (NULL), ('NULL'), ('a b')) SELECT v FROM c WHERE v = '';
