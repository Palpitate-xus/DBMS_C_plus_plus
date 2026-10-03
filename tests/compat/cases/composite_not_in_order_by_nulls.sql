-- The semi-join plan must preserve explicit and default NULL ordering.
DROP TABLE IF EXISTS diff_notin_order_outer
DROP TABLE IF EXISTS diff_notin_order_inner
CREATE TABLE diff_notin_order_outer (a INT, b INT)
INSERT INTO diff_notin_order_outer VALUES (1, 2), (NULL, 3), (4, 5), (3, NULL)
CREATE TABLE diff_notin_order_inner (a INT, b INT)
SELECT a, b FROM diff_notin_order_outer WHERE (a, b) NOT IN (SELECT a, b FROM diff_notin_order_inner) ORDER BY a NULLS FIRST
SELECT a, b FROM diff_notin_order_outer WHERE (a, b) NOT IN (SELECT a, b FROM diff_notin_order_inner) ORDER BY a DESC NULLS LAST
SELECT a, b FROM diff_notin_order_outer WHERE (a, b) NOT IN (SELECT a, b FROM diff_notin_order_inner) ORDER BY a DESC
DROP TABLE IF EXISTS diff_notin_order_text
DROP TABLE IF EXISTS diff_notin_order_text_empty
CREATE TABLE diff_notin_order_text (v TEXT)
INSERT INTO diff_notin_order_text VALUES (NULL), (''), ('a'), ('b')
CREATE TABLE diff_notin_order_text_empty (v TEXT)
SELECT v FROM diff_notin_order_text WHERE v NOT IN (SELECT v FROM diff_notin_order_text_empty) ORDER BY v DESC NULLS LAST
