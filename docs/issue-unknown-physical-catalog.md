# UNKNOWN has a real physical catalog identity

The first pg_typeof candidate publication 43092 actually exits 1: its native
fixture aborts 134 and its new wire fixture fails valid UNKNOWN controls with
42704. The copied builtin catalog has no OID 705 row. Do not fabricate an OID
only inside the evaluator or convert UNKNOWN inputs to TEXT to pass the test.

Add the genuine `pg_catalog.unknown` pseudo-type row to the shared immutable
physical definition producer. Actual PostgreSQL 18.6, verified server180006,
confirms OID705, owner10, length-2, by-reference, categoryX, alignmentc,
plain storage, no element/array/collation/base type, and input/output/receive/
send OIDs109/110/2416/2417. This does not invent an array or a table-column
codec for UNKNOWN. Normal bootstrap publishes a genuinely missing row.

Own normal catalog compilation 22168 exits0. Matching normal O2 native38999
exits0: physical shape, pure cold declarations without catalog/disk creation,
actual warm rows, persisted rows and a separate engine's copied disk metadata
all pass. The initial pg_typeof candidate's 9native/10wire terminal is retained:
8native and8wire pass,3 entries fail, complete original default protocol passes,
all58/repeat/source/frozen terminal checks0. It is not a full-suite pass.

Artifacts: `/tmp/dbms-having-grammar.MPk64yc0`.
Build: `unknown-own-catalog-build.log`; native: `unknown-catalog.log`.
Initial publication: `pg-typeof-publication9-10.log`, binary919a7c/source5069d8.
Ordinary parameter identity, mixed USER and pg_typeof receiver fixes remain
separate work. Original273 item statuses are unchanged; no push or Actions.
