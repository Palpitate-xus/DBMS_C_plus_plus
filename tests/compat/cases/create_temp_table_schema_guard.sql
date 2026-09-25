CREATE TEMP TABLE public.diff_temp_schema_guard (id integer);
CREATE TEMP TABLE IF NOT EXISTS public.diff_temp_schema_guard (id integer);
CREATE TEMP TABLE diff_missing_temp_schema_guard.t (id integer);
CREATE TEMP TABLE IF NOT EXISTS diff_missing_temp_schema_guard.t (id integer);
SELECT 1;
