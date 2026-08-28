-- regex posix classes, array concat, array functions
SELECT 'abc' ~ '[[:alpha:]]+'
SELECT 'abc123' ~ '^[[:alpha:]]+[[:digit:]]+$'
SELECT 'a.c' ~ 'a\.c'
SELECT 'x' ~ '\w'
SELECT array[1,2] || array[3,4]
SELECT array_position(array[1,2,3], 2)
SELECT array_dims(array[1,2,3])
SELECT array['a','b'] || array['c']
SELECT array_length(array[1,2,3], 1)
SELECT string_to_array('a,b,c', ',')
SELECT array_to_string(array[1,2,3], '-')
SELECT array_upper(array[1,2,3], 1)
SELECT array_lower(array[1,2,3], 1)
