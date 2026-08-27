-- to_char MI trailing minus
SELECT to_char(-42, '9999MI')
SELECT '|' || to_char(-42, 'FM9999MI') || '|'
SELECT to_char(-482, 'MI9999')
SELECT to_char(482, 'MI9999')
