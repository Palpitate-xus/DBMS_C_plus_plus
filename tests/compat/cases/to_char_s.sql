-- to_char S sign position
SELECT '|' || to_char(-42, 'S9999') || '|'
SELECT '|' || to_char(42, 'S9999') || '|'
SELECT '|' || to_char(42, '9999S') || '|'
SELECT '|' || to_char(42, 'FM9999S') || '|'
SELECT '|' || to_char(42, 'FMS9999') || '|'
