# Prepared CASE/cast projection labels

## Defect

The query binder described a CASE target as `?column?`. It also lost names
through casts and COLLATE. This broke references to the implicit CTE column
`"case"`, `int8`/`int4` cast labels, or a cast's strong underlying function
name. Activating the ordinary typed CASE consumer exposed the same wrong
labels on the wire even though its first value/type matrix already passed.
This is a separate metadata root cause, not another CASE equality change.

The binder now derives labels from structured AST roles before expression
transformation. Column/function names are strong; CASE/ARRAY/ROW and typed
constants supply weak labels. Casts retain a strong operand name, otherwise
use the builtin catalog type label. COLLATE preserves its operand label;
ordinary arithmetic operators do not. Explicit aliases remain authoritative
and keep their canonical quoted identity. No SQL execution or datum value is
used to infer a label. Custom type catalog rendering remains a broader boundary.

## Actual proof

Artifact root: `/tmp/dbms-simple-case-binding.KtyWx31T`.

The original native run `label.native.baseline.log` (20796, exit 134) reported
five wrong labels and an uncaught CTE `42703`. The final unchanged-value
fixture catches that metadata error explicitly:
`label.native.baseline.final-fixture.log` (23461, exit 134) reports six failed
assertions. The candidate has five matching `-O2` native successes in
`build.labels.log` (9477, exit 0): the new projection test, VALUES common type,
simple CASE equality, CASE common type, and query binding.

Strict isolated PostgreSQL 18.6 (`180006`) passes `label.reference18.log`.
The first protocol baseline timed out while creating its fixture table,
before any issue assertion (`label.wire.baseline.log`, 99058, exit 1).
That log is retained and is not counted as a projection defect. An unchanged
retry at the original timeout reached every assertion and failed 12 of them
(`label.wire.baseline.retry.log`, exit 1): four missing CTE column names each
lost the expected successful state, returned row and OID 20. The quoted
explicit alias positive control already passed. The fixed candidate passes
all original assertions (`label.wire.candidate.log`, exit 0).

The source change has no public header/layout change. `labels/obj` contains a
fresh binder object and fresh stubs; the immutable current ordinary candidate's
other 57 source/header/flag hashes match. The ordinary candidate itself uses
the all-58 fresh simple CASE API basis plus matching freshly compiled owned
TUs, including ROOT's independently fixed RETURNING ARRAY and UPDATE sources.
`labels/{sources,headers}.audit.txt`, `labels/other57.sources.audit.txt`, and
`labels/binary.sha256` record the exact combination. The future ordinary CASE
source commit is required separately; labels alone do not activate that route.
