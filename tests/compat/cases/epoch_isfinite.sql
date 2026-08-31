SELECT extract(epoch from interval '3 days');
SELECT extract(epoch from interval '1 mon');
SELECT extract(epoch from interval '2 hours 30 min');
SELECT isfinite(interval '3 days');
SELECT isfinite(interval 'infinity');
SELECT extract(epoch from timestamp '1970-01-02 00:00:00');
