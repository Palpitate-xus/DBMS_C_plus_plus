CREATE TABLE diff_literal_boundaries(id INT);
INSERT INTO diff_literal_boundaries VALUES(1),(2);
SELECT id,E'Keep\' FROM words, (data)' AS data FROM diff_literal_boundaries ORDER BY id;
SELECT id,$body$FROM as fake, (data)$body$ AS data FROM diff_literal_boundaries ORDER BY id;
SELECT id,'simple, comma' AS data FROM diff_literal_boundaries ORDER BY id;
SELECT id,'parenthesis ) , ( text' AS data FROM diff_literal_boundaries ORDER BY id;
SELECT id,'say '' FROM '' as text' AS data FROM diff_literal_boundaries ORDER BY id;
SELECT id,'ordinary text' AS data FROM diff_literal_boundaries ORDER BY id;
DROP TABLE diff_literal_boundaries;
