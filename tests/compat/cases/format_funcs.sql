-- regexp_replace, format specifiers
SELECT regexp_replace('Hello World', '[A-Z]', 'x', 'g')
SELECT regexp_replace('Hello World', '[A-Z]', 'x')
SELECT format('%s has %s', 'a', 'b')
SELECT format('%I', 'some table')
SELECT format('%L', 'it''s')
SELECT format('%s%%', 'x')
SELECT format('%L', 'plain')
SELECT format('%I', 'ok_name')
SELECT 'don''t panic'
