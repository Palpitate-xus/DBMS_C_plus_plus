-- Strict integer coercion in string+int arithmetic (PG 22P02)
SELECT '1' + 2;
SELECT 7 - '3';
SELECT '2024-03-15' + 7;
SELECT 'abc' + 1;
SELECT '1.5' + 1;
SELECT '2024-03-15' - 7;
SELECT 7 + '2024-03-15';
-- both-operand unknown strings: PG cannot resolve the operator
SELECT '1' + '2';
SELECT 'a' + 'b';
-- bare numeric literals still resolve as numeric
SELECT 1.5 + 1;
SELECT 1.5 + '1';