-- to_char TH V RN templates
SELECT '|' || to_char(42, '9999TH') || '|'
SELECT '|' || to_char(1, '9999th') || '|'
SELECT '|' || to_char(4.2, '9V99') || '|'
SELECT '|' || to_char(42, '9V99') || '|'
SELECT '|' || to_char(42, 'RN') || '|'
SELECT '|' || to_char(2026, 'RN') || '|'
