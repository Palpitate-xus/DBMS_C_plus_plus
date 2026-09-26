-- A CAST nested inside a comparison must not name the projected column.
SELECT CAST('apple' AS text) < 'Zoo';
SELECT CAST('apple' AS text) = 'apple';
