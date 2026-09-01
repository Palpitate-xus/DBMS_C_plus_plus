SELECT '{"a": 1}'::json -> 'a' AS x1;
SELECT '{"a": {"b": 2}}'::json -> 'a' -> 'b' AS x2;
SELECT '[1, 2, 3]'::json ->> 1 AS x3;
SELECT '[1, 2, 3]'::json -> 1 AS x4;
SELECT '{"a": 1}'::json ->> 'a' AS x5;
SELECT json_build_object('a', 1) -> 'a' AS x6;
SELECT '{"a": {"b": 5}}'::json #> '{a,b}' AS x7;
SELECT '{"a": {"b": 5}}'::json #>> '{a,b}' AS x8;
