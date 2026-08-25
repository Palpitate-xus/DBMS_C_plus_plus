-- error conditions and SQLSTATE surfaces
SELECT 1/0
SELECT 'abc'::int
SELECT * FROM no_such_table
SELECT nonexistent_fn(1)
INSERT INTO no_such_table VALUES (1)
SELECT sum('abc')
