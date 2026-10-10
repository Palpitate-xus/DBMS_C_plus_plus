# Native date fixture constructs genuine SQL-value ASTs

Root full85873's date_functions native fails134 at a manually constructed
ordinary FunctionCallExpr("current_date"), with intrinsic kindNone. The parser
and shared scalar helper actually retain SQL-value grammar correctly (probe
34231). PostgreSQL18.6 rejects current_date() with42601; a keyword intrinsic
must not be restored as a fake zero-argument callable merely to pass this test.

Add a separate fixture helper that obtains the genuine intrinsic AST through
the real binding parser and asserts its non-None kind. Use it for current_date,
current_timestamp,localtimestamp,current_time,localtime and the later age/date
control. Keep callFn unchanged for ordinary callable functions. All original
date/time values, types, before/after clock boundaries, NULL, overlaps and age
assertions remain intact; no negative control or section is removed.

Matching normal O2 native26099 exits0, with all original sections passing.
Actual verified180006 independently confirms the five keyword output types;
existing keyword reference33 also preserves the invalid empty-call syntax.
No production change, registry workaround, Main rebuild or family-completion
claim. Root still runs its frozen53native/36wire publication without this test
edit; do not import it until that publication is terminal.

Artifacts:/tmp/dbms-having-grammar.MPk64yc0.
Native:date_functions.sql-value-ast.O2.
Reference:pg18_sql_value_types_probe.py.
Original Root failure:root-full775-437.log in /tmp/dbms-root-sql-value.x5X8O5vm.
Original273 requirements/statuses and15 user-deferred security/TDE items remain;
no push or Actions activation.
