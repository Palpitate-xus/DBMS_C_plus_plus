-- An explicit empty signature must not delete a differently typed function.
CREATE OR REPLACE FUNCTION pgdiff_param_fn(x int) RETURNS int LANGUAGE SQL AS $$ SELECT x + 1 $$;
DROP FUNCTION IF EXISTS pgdiff_param_fn();
SELECT pgdiff_param_fn(1);
