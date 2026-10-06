# Qualified view target dispatch: independently reproduced, still open

While verifying the independent native DML session-scope repair, the exact
positive fallback control `INSERT INTO public.session_view VALUES (9)`
reports `Table public.session_view not exist` instead of delegating the view
to its dedicated rewrite/trigger owner. The view was actually created in
public and its unqualified counterpart exercises the existing fallback path.

Actual retained failure is
`/tmp/dbms-checked-native-fixtures.xmBivqmc/dml-session-final.log`:
the materialized-view original55000/no-effects controls and typed native
session-success assertions complete, then the newly added qualified view
fallback assertion aborts. Source checks confirm the legacy INSERT target
guard tests viewExists against requested spelling rather than resolved
physical/logical relation identity. This is a separate target-dispatch bug,
not justification to weaken the source-session or view fallback contract.

The session-scope verification uses the already-supported unqualified view
control to establish caller-context restoration. The exact qualified SQL and
its expected fallback behavior remain open here for an independent permanent
reproduction/fix. Schema-qualified and shadowed view/materialized-view
UPDATE/DELETE/INSERT targets also require actual tests before wider closure;
no claim about those unexecuted shapes is made.
