# TIMETZ[] lacks its builtin array identity

Independent builtin metadata repair after 0bf6da71, not complete array-value
codec, operator or type-catalog graph support.

The complete strict PostgreSQL 18.6/180006, XML-enabled/en_US reference returns
OID1270, size-1 and typmod-1 for physical TIMETZ[] on empty and SQL-NULL rows.
The original58-normal-O2 baseline967ed6 reports25. After the separate modifier
and source-origin repairs, the frozen5e806d candidate still has exactly six
failures, TIMETZ[] identity in all three description modes on those two shapes.
Its native test8297 also actually aborts (134): mapBuiltinTypeNameToOid returns0.

Both TIMETZ array aliases now resolve1270. Catalog bootstrap supplies _timetz
with typcategoryA, typlen-1 and actual scalar typelem1266; persistence/cold
reload preserves the link. No public header/layout/new-TU changes were made.
This does not retrofit every other builtin array catalog graph or claim
arbitrary TIMETZ array datum input/output support.

Normal-O2 build51383 and repeat passed; all58 source/header/flags/object
signatures and stamp passed. Frozen source/object/binary archive:
`/tmp/dbms-physical-array-typmod.YCjPAKiX/timetz-final-immutable.qGuOFcnL/`
SHA256 `c98ad0f136ec8a66c8b72605ca490a3eb35435262c948903587a5bd91125d7b2`.
Four native tests27875 all passed (new identity and existing modifier catalog,
catalog service and type aliases). Full unchanged12-shape/24-base array
metadata fixture, original nine-shape array fixture and focused modifier
fixture all passed; the full permanent12-shape strict180006 reference passed.
The fixture is registered only now that its complete assertions pass, with
the old70 and intermediate6 failure logs retained.

The source-origin fixture's quoted uppercase range alias runtime42P01 remains
separately OPEN and unregistered; this type-identity commit does not relabel it.
Artifact directory `/tmp/dbms-physical-array-typmod.YCjPAKiX/` retains all logs.
Default15s remains unchanged, servers are stopped in finally and tmpfs testing
establishes semantics rather than disk-I/O performance closure.
