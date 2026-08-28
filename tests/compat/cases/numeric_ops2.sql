-- trunc scale, power, mod semantics
SELECT trunc(42.87, 1)
SELECT trunc(-42.87, 1)
SELECT trunc(42.87::numeric, 1)
SELECT power(2, 10)
SELECT power(2.0, 0.5)
SELECT mod(10, 3)
SELECT mod(-10, 3)
SELECT 10 % 3
SELECT power(9, 0.5)
SELECT power(2.5, 2)
SELECT power(2, 3)
SELECT power(3.0, 0.25)
