-- LOWER's text result inherits the explicit collation of its input.
SELECT lower('APPLE' COLLATE "C") < 'Zoo' AS result;
SELECT lower('APPLE' COLLATE "default") < 'Zoo' AS result;
