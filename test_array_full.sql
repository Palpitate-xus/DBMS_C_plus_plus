USE DATABASE testdb;

-- Test array type with functions
CREATE TABLE test_array (id INT, nums INT[]);

INSERT INTO test_array (id, nums) VALUES (1, '[10,20,30]');
INSERT INTO test_array (id, nums) VALUES (2, '[100,200]');

-- Test array_get function (1-based index)
SELECT id, array_get(nums, 1) AS first_elem, array_get(nums, 2) AS second_elem FROM test_array;

-- Test array_length function
SELECT id, array_length(nums) AS len FROM test_array;

-- Test array_contains function
SELECT id FROM test_array WHERE array_contains(nums, '20') = '1';

-- Test with VARCHAR array
CREATE TABLE test_str_array (id INT, tags VARCHAR[]);
INSERT INTO test_str_array (id, tags) VALUES (1, '["hello","world"]');
SELECT id, array_get(tags, 1), array_get(tags, 2) FROM test_str_array;
SELECT id FROM test_str_array WHERE array_contains(tags, 'world') = '1';

-- Cleanup
DROP TABLE test_array;
DROP TABLE test_str_array;
