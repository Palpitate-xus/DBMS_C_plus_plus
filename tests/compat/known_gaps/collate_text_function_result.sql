-- Text-producing builtins inherit explicit collation from their arguments.
SELECT ltrim('  apple' COLLATE "C") < 'Zoo' AS result;
SELECT replace('apple' COLLATE "C", 'p', 'p') < 'Zoo' AS result;
SELECT left('apple' COLLATE "C", 5) < 'Zoo' AS result;
SELECT reverse('elppa' COLLATE "C") < 'Zoo' AS result;
SELECT replace('apple' COLLATE "C", 'p' COLLATE "default", 'p') AS result;
SELECT strpos('apple' COLLATE "C", 'p' COLLATE "default") AS result;
SELECT concat('a' COLLATE "C", 'b' COLLATE "default") AS result;
SELECT concat('app' COLLATE "C", 'le') < 'Zoo' AS result;
