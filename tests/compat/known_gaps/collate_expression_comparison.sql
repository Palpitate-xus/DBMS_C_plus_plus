-- A collated scalar comparison must evaluate to boolean, not its left text.
SELECT 'apple' COLLATE "default" < 'Zoo' COLLATE "default";
