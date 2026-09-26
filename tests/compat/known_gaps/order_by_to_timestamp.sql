-- Timestamp parsing, not source text, defines the sort key.
CREATE TEMP TABLE diff_order_to_timestamp(v text);
INSERT INTO diff_order_to_timestamp VALUES ('01/01/2021'), ('01/12/2020');
SELECT v FROM diff_order_to_timestamp ORDER BY to_timestamp(v, 'DD/MM/YYYY');
DROP TABLE diff_order_to_timestamp;
