SELECT CAST(abs(-1) AS numeric),abs(-1)::numeric,CAST(COALESCE(NULL,1) AS numeric),coalesce(NULL,1)::numeric,CAST(NULLIF(1,2) AS numeric),CAST(GREATEST(1,2) AS numeric),CAST(LEAST(1,2) AS numeric),CAST(CAST(abs(-1) AS numeric) AS text);
SELECT CAST(CAST(1 AS integer) AS text),CAST(1+2 AS text),CAST(abs(-1)+1 AS text),CAST(abs(-1) AS numeric) AS value;
