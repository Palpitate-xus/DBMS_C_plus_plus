-- Default-locale scalar text ordering must match table predicates.
SELECT 'apple' < 'Zoo';
SELECT 'Zoo' > 'apple';
SELECT 'apple' = 'apple';
