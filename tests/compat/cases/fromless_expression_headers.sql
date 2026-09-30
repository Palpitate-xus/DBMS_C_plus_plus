SELECT -1::numeric,1::numeric+2::numeric,(1+2)::numeric,abs(-1)+1,-abs(-1),'a'::text||'b'::text,'a'::text;
SELECT abs(-1),CAST(1+2 AS numeric),(1::numeric+2::numeric) AS amount;
