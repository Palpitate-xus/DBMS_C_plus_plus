-- Polymorphic scalar functions must order by their typed result.
CREATE TEMP TABLE diff_order_polymorphic(v integer);
INSERT INTO diff_order_polymorphic VALUES (4), (2), (NULL);
SELECT v FROM diff_order_polymorphic ORDER BY least(v, 3);
SELECT v FROM diff_order_polymorphic ORDER BY greatest(v, 3);
SELECT v FROM diff_order_polymorphic ORDER BY coalesce(v, 0);
SELECT v FROM diff_order_polymorphic ORDER BY nullif(v, 4);
DROP TABLE diff_order_polymorphic;
CREATE TEMP TABLE diff_order_polymorphic_text(v text);
INSERT INTO diff_order_polymorphic_text VALUES ('b'), ('a');
SELECT v FROM diff_order_polymorphic_text ORDER BY least(v, 'z');
DROP TABLE diff_order_polymorphic_text;
CREATE TEMP TABLE diff_order_polymorphic_numeric(v numeric);
INSERT INTO diff_order_polymorphic_numeric VALUES (10.2), (2.2);
SELECT v FROM diff_order_polymorphic_numeric ORDER BY coalesce(NULL, v);
SELECT v FROM diff_order_polymorphic_numeric ORDER BY least(NULL, v);
SELECT v FROM diff_order_polymorphic_numeric ORDER BY greatest(NULL, v);
DROP TABLE diff_order_polymorphic_numeric;
