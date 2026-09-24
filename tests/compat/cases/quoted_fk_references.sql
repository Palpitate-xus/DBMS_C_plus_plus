DROP TABLE IF EXISTS public.diff_qfqi_child;
DROP SCHEMA IF EXISTS diff_qfqi CASCADE;
CREATE SCHEMA diff_qfqi;
CREATE TABLE diff_qfqi.parent (id integer PRIMARY KEY);
CREATE TABLE public.diff_qfqi_child (id integer REFERENCES "diff_qfqi"."parent"(id));
INSERT INTO diff_qfqi.parent VALUES (1);
INSERT INTO public.diff_qfqi_child VALUES (1);
SELECT id FROM public.diff_qfqi_child;
