-- Casts between collatable text types preserve an explicit input collation.
SELECT CAST(('apple' COLLATE "C") AS text) < 'Zoo' AS result;
SELECT ('apple' COLLATE "C")::text < 'Zoo' AS result;
SELECT CAST(('apple' COLLATE "default") AS text) < 'Zoo' AS result;
SELECT CAST((NULL::text COLLATE "C") AS text) < 'Zoo' AS result;
