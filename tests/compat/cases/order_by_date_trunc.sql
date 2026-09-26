-- ORDER BY date_trunc must sort on the computed timestamp, not a missing key.
CREATE TEMP TABLE diff_order_date_trunc(v timestamp);
INSERT INTO diff_order_date_trunc VALUES ('2020-03-15 12:00:00'), ('2020-01-01 00:00:00');
SELECT v FROM diff_order_date_trunc ORDER BY date_trunc('month', v);
DROP TABLE diff_order_date_trunc;
