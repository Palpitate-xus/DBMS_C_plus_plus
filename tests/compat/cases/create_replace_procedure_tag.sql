-- Replacing a procedure returns PostgreSQL's CREATE PROCEDURE command tag.
CREATE PROCEDURE pgdiff_tag_proc() LANGUAGE SQL AS $$ SELECT 1 $$;
CREATE OR REPLACE PROCEDURE pgdiff_tag_proc() LANGUAGE SQL AS $$ SELECT 2 $$;
DROP PROCEDURE pgdiff_tag_proc();
