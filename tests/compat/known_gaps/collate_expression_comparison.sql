-- A collated scalar comparison must evaluate to boolean, not its left text.
SELECT 'apple' COLLATE "default" < 'Zoo' COLLATE "default";
SELECT 'apple' COLLATE "C" < 'Zoo' COLLATE "C";
SELECT 'apple' COLLATE "C" < 'Zoo' COLLATE "default";
SELECT 'apple'::text COLLATE "C" < 'Zoo'::text COLLATE "C";
SELECT 1 COLLATE "C";
SELECT 'apple' COLLATE "missing_collation";
