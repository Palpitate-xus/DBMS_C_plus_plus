-- OR REPLACE does not change CREATE FUNCTION's command tag.
CREATE OR REPLACE FUNCTION pgdiff_replace_fn() RETURNS int LANGUAGE SQL AS $$ SELECT 2 $$;
SELECT pgdiff_replace_fn();
