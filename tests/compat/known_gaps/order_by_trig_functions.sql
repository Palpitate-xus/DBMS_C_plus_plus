-- Trigonometric ORDER BY keys are results, not the input argument.
CREATE TEMP TABLE diff_order_trig(v numeric);
INSERT INTO diff_order_trig VALUES (1.5), (4.5);
SELECT v FROM diff_order_trig ORDER BY sin(v);
SELECT v FROM diff_order_trig ORDER BY cos(v);
SELECT v FROM diff_order_trig ORDER BY tan(v);
DROP TABLE diff_order_trig;
