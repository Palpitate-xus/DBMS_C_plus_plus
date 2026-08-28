-- substring-from, overlaps, array ops
SELECT substring('hello world' from 7)
SELECT substring('hello world' from 7 for 3)
SELECT ('2026-01-01'::date, '2026-01-05'::date) overlaps ('2026-01-04'::date, '2026-01-10'::date)
SELECT ('2026-01-06'::date, '2026-01-08'::date) overlaps ('2026-01-01'::date, '2026-01-05'::date)
SELECT ('2026-01-01'::date, '2026-01-02'::date) overlaps ('2026-01-02'::date, '2026-01-03'::date)
SELECT ('2026-01-01'::date, '2026-01-01'::date) overlaps ('2026-01-01'::date, '2026-01-01'::date)
SELECT array[1,2,3] @> array[1,2]
SELECT array[1,2,3] @> array[1,5]
SELECT array[1,2,3] && array[3,4]
SELECT array[1,2,3] && array[4,5]
SELECT array[1,2,3] <@ array[1,2,3,4]
SELECT cardinality(array[1,2,3])
