-- to_char EEEE scientific
SELECT '|' || to_char(42, '9EEEE') || '|'
SELECT '|' || to_char(0.0042, '9.99EEEE') || '|'
SELECT '|' || to_char(-42, '9.99EEEE') || '|'
