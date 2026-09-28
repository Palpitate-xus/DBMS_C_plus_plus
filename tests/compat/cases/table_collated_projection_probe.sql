-- A selected table column may carry an explicit output collation.
CREATE TABLE diff_collated_projection(v text);
INSERT INTO diff_collated_projection VALUES ('a b'), ('NULL');
SELECT v COLLATE "C" AS v FROM diff_collated_projection ORDER BY v;
SELECT v COLLATE "C" AS x FROM diff_collated_projection ORDER BY x;
SELECT v COLLATE "C" FROM diff_collated_projection ORDER BY v DESC;
SELECT v COLLATE "C" AS v FROM diff_collated_projection ORDER BY v COLLATE "default";
DROP TABLE diff_collated_projection;
