-- typed numeric casts
DROP TABLE IF EXISTS diff_nc1
CREATE TABLE diff_nc1 (v numeric)
INSERT INTO diff_nc1 VALUES (1.1), (22.345)
SELECT v::numeric(4,2) FROM diff_nc1
SELECT v::numeric FROM diff_nc1
SELECT cast(v as numeric(4,2)) FROM diff_nc1
SELECT 1.1::numeric(4,2), 22.345::numeric(6,2), 7::numeric(4,2) FROM diff_nc1
