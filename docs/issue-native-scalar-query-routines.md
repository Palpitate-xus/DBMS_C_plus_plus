# Native scalar-query routine ownership

The no-host scalar child path used `plpgsqlQueryNative`, whose function
validator recognized evaluator builtins only. A real independent engine
successfully stored `native_where_failure` (metadata, zero-argument arity and
PL body were read back), but `(SELECT native_where_failure())` incorrectly
raised 42883 instead of executing its P0001 exception.

The native subset now validates canonical scalar routines through the shared
metadata-only resolver with the actual database and engine. Output and sort
types are inferred from the original AST rather than serialized SQL; quoted
names, explicit schema identity and routine return types remain intact. All
projection, WHERE and ORDER namespaces are checked before any callback can
execute. Actual callbacks are bound after that preparation, once per borrowed
projection/ORDER root. Stored `public.sum()` is not mistaken for a builtin
aggregate solely because of its basename.

## Evidence and limits

Artifacts are retained under
`/tmp/dbms-ordinary-scalar-consumer.I1bn6m0R`. The independently added native
test failed on baseline efc501b9: session 82118 exited 134, actual 42883 and
expected P0001. The unchanged ordinary-plan P0001 control also failed in
89884/22695; 22695 logged the existing metadata and body. The first native
candidate passed five tests in 46779, but an expanded `public.sum()` control
really failed with 0A000 in 65280. Both failures are retained.

Final source rebuild session 55337 exited 0. No public API/header/layout
changed: TableManage was rebuilt with O0 against the immutable all-57 fresh
efc501b9 source/header origin; separate native-only and WHERE-plus-native
binaries were linked. This is not a fresh optimized ROOT combined build.
The native-only binary SHA256 is
`e1cf145e80a4196380d15e7a50d2c5b7d4d988386f725f54fea85d00e50081e3`.
Matching native session 47467 exited 0 for seven tests: the new native
routine test, the unchanged ordinary-plan P0001 control, PL query host,
prepared scalar context, prepared execution, prepared array metadata, and
arithmetic result metadata. The new test also checks quoted public routines,
BIGINT descriptors, STRICT typed NULL, lazy CASE/LIMIT 0, unknown callee
preparation before a fatal projection, schema/case isolation, and a table
WHERE/ORDER output alias.
Serial protocol group 63956 exited 0. On the native-only binary, the three
adjacent scripts (fromless structured, PL SELECT INTO, WITH scalar child)
passed; then the WHERE-plus-native binary passed its new WHERE matrix and
three adjacent stored-function WHERE/atomicity/ORDER scripts. No two owned
servers were run in parallel. The server uses its existing full SQL host,
so these adjacent wire checks are not described as a hostless-wire red/green.

This retains the native subset's explicit aggregate/CTE/join/window guards.
It does not broaden the hostless SQL dispatcher, repair EXTRACT grammar
lowering, or prove every native multirow writer statement's transaction
ownership. Shared SQL type/signature families and ordinary ORDER/scalar
query consumers remain partial and separately tracked.
