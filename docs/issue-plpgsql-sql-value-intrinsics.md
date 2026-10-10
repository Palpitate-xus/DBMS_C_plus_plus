# PL scalar binding retains actual SQL-value intrinsics

The unchanged original plpgsql_quoted_scalar_binding native82335 exits1:
`current_date IS NOT NULL AND current_timestamp IS NOT NULL AND localtimestamp
IS NOT NULL` fails42883. The public parser is not the cause: actual probe34231
shows both ordinary and binding parsers retain intrinsic kinds6/9/10, and the
shared ExprHelper successfully evaluates the same expressions.

The independent PL scalar classifier nevertheless requires a callable registry
entry. SQL-standard value expressions are not such callbacks. Validate and
retain the parser's intrinsic kind instead of resolving it as a stored/builtin
callable routine. After lowering genuine calls to their existing private slots,
use the normal scalar binder to preserve the actual engine owner for intrinsic
namespace lookup as well. Do not register fake current_date/user callbacks or
infer intrinsic identity from a callable spelling.

Own storage CPP57091 exits0. Four matching native entries8479 pass: the whole
original quoted PL fixture, actual engine-owner, scalar-resolver and PL query-
host neighbors. The PL fixture additionally keeps time/schema/catalog and
precision positives, invalid empty-call syntax and negative-precision errors.
An initial newly authored negative-precision expectation22023 failed27581;
actual verified18.6/server180006 directly confirms current_time(-1),
current_timestamp(-1),localtime(-1) are syntax42601. Correct the new expectation
to the genuine oracle, retain that strict error control, and preserve the
initial authoring failure. All original positives/negatives remain unchanged.

Seven whole wire fixtures48517 exit0: quoted PL, query binding, SELECT INTO,
original keyword33, procedure32, DROP20 and Gen78. No public header/layout
change here; Main and all other object receipts retain their actual current
production provenance. Normal/repeat/source/cache/frozen checks cover all58
own objects. No foreign object or binary copying. Complete original default
and expanded51/33 were not rerun in this specific storage generation; do not
reuse old passes as new proof.

Artifacts:/tmp/dbms-having-grammar.MPk64yc0.
Compile:plpgsql-sql-value-own-storage-build.log.
Build:plpgsql-sql-value-build.log.
Reference precision probe:pg18_precision_probe.py.
Native binaries:*.plpgsql-sql-value-corrected.O2.
Frozen:dbms_main.plpgsql-sql-value.frozen.

Ordinary pg_typeof receiver, date native AST fixture, quoted enum, catalog
image race, recovery and EXPLAIN gaps remain. Original273 status/scope are not
reduced; only15 explicit security/TDE requirements are deferred. No push or
Actions activation.
