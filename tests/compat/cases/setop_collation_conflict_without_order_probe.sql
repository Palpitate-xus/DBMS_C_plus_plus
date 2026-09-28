-- A DISTINCT set operation compares text even without a final ORDER BY.
SELECT 'a' COLLATE "C" AS v UNION SELECT 'b' COLLATE "default" AS v;
SELECT 'a' COLLATE "C" AS v INTERSECT SELECT 'b' COLLATE "default" AS v;
SELECT 'a' COLLATE "C" AS v EXCEPT SELECT 'b' COLLATE "default" AS v;
-- UNION ALL does not need to compare values, so retain both typed rows.
SELECT 'a' COLLATE "C" AS v UNION ALL SELECT 'b' COLLATE "default" AS v;
