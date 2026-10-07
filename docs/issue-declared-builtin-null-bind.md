# Genuine NULL Bind datums retain their declared builtin type

Status: finite eleven-builtin NULL execution defect independently repaired in
the private signed-FETCH/Source84 epoch. Root's later Source100/public-consumer
ABI is not proved by these receipts. TEXT width is the separate parent issue
`7b3eb6bdb3a5fc7f5dc74595aa958c2c519c875e`. Master, push and Actions are untouched.

## Root cause and finite production change

Real extended Parse declares a parameter OID. Bind reads a real length `-1`
NULL datum (no value bytes) and previously lowered every such datum to the
untyped SQL token `NULL`, except in the pre-existing bare VALUES path. A real
scalar child then had an UNKNOWN output descriptor and the public prepared
cursor correctly refused it with `XX000`. This occurs on empty and populated
physical tables; it cannot be repaired by a first-row type guess.

Exactly the existing builtin cast-lowering condition now admits either bare
VALUES or a genuine `valueLength == -1` datum. The existing cast map is not
changed: boolean, bigint, smallint, integer, text, real, double precision, date,
timestamp, timestamptz and numeric. Its type comes only from the original
`preparedParameterTypes[i]`, never NULL bytes or a result row. The existing
parameter-occurrence substitution remains the receiver: no whole-string SQL
replacement, SQL predicate rewrite, new parameter constant analysis or source
scan was introduced. Non-NULL parameter decoding, unmapped/unspecified OIDs,
custom OIDs and all existing VALUES lowering are untouched. In particular,
UNKNOWN scalar literals are not silently finalized to TEXT in the cursor.

This is one small NetworkServer.cpp hunk, with no header or other production
source changes. When composing it into a later Root Network epoch, retain all
integer preparation, BIT Bind codec and f5 occurrence/source-use guards; do
not replace the old Network file as a whole.

## Permanent strong coverage

`typed_null_bind_protocol_e2e_test.py` uses genuine named Parse, actual declared
ParameterDescription, NULL Bind length `-1` in both text/binary formats,
statement/portal Describe, Execute and Close. It checks all eleven OIDs,
type widths, typmods, result formats, names, zero computed-expression origin,
NULL flags, row cardinality and complete command tags. Controls cover direct
SELECT, original VALUES typing, Simple Query typed NULL, empty/nonempty real
PK tables, parent FALSE/LIMIT 0 demand, >2^53 BIGINT, binary INTEGER, non-NULL
empty text/string NULL/embedded-quote-and-parameter-looking bytes, repeated
actual parameter occurrences and harmless SQL-looking string data.

The outer explicit CAST in scalar controls deliberately provides a known
legacy outer descriptor before and after this fix. It isolates the real inner
NULL execution defect; it does not claim the separate uncast scalar FETCH
Describe defect is repaired. Strong original uncast FETCH assertions remain
unchanged and red in the adjacent full driver.

An actual volatile writer establishes zero calls at Parse/Bind/Describe and
exactly two calls at Execute (the required ties lookahead), with sequence
value 1 before Execute and 3 afterwards. Explicit savepoints on both servers
only recover real errors, so every subsequent control can still run.

`typed_null_parameter_execution_test.cpp` independently checks 68 genuine
whole-bound public-graph controls with real typed QueryBindingDatum NULL
frames, actual one-slot/occurrence ownership (including three CASE uses),
typed cursor cells, empty/nonempty relations, parent demand and exact writer
lookahead/no-metadata-effects. Fresh native drivers and a fresh stub are used.
The e2e registry includes the new wire driver; no original fixture or deadline
was edited.

## Exact immutable evidence

Artifact root: `/tmp/dbms-typed-null-bind.lMtqOUwj`.
Owned PostgreSQL reference: version `180006`, C/libc locale, port 15486.
Baseline is the metadata-only parent binary `dbms_main.text-width.frozen`,
SHA-256 `91a800ff915293ff8c81673bc30b426c4f5c256e0f4436bf06ea6a941b2a0bdb`.
Candidate `dbms_main.typed-null.frozen`, SHA-256
`0f9921cd5e7ac65524b5eb2cca787a7207e63c48045a8ad9aac163f5923bbf0b`.

| Gate | Actual terminal result |
| --- | --- |
| `typed-null-strict18-v3.log` | 0; all 1001 controls, no failures |
| `typed-null-baseline-v3.log` | 1; all 956 controls, exactly 44 genuine child Execute XX000 records |
| `typed-null-normal-v1.log` | 0; sole Network translation unit genuinely rebuilt, then repeat-cache proof |
| `typed-null-whole26-v1.log` | 1; all 26 suites attempted, only original FETCH18 and DISTINCT suites red |
| New driver inside the full whole26 log | 0; all 1000 controls, no failures (reference alone has SET LOCAL search_path) |
| `typed-null-native19-v3.log` | 0; all 19 native drivers, including all new 68 exact slot/occurrence controls |
| `typed-null-native-baseline-v3.log` | 0; new native 68 with original donor production objects and fresh driver/stub |

`verify-typed-null-current.sh` gives the complete native/wire lists and checks
every compatible donor source/header/flag/TLS/manifest/object receipt/cache
signature/frozen byte before reuse. Donor is clean signed-FETCH commit
`4757cc3a2cbccb6fb535d0a975e55c8d858c89dd`, whose frozen SHA-256 is
`d3d54e73f7f6b3eac1cacf1dfb301931f613a9910ada38433b08577627e4c3bb`.
All 57 non-Network objects and every header are byte-proven compatible. Each
of the two independent Network issue epochs compiled Network genuinely;
neither stage claims another genuinely-fresh-58 build. The signed-AST donor
epoch itself has separately recorded genuine all-58 compilation evidence.

## Retained neighbours, capability limits and author corrections

Original full FETCH assertions still report 18 unrelated nonnegative
source-free EXISTS/unprojected ORDER key/static scalar-descriptor records.
Original DISTINCT expects `0A000` where reference and candidate return `21000`.
Neither assertion was weakened. The already-observed original view-trigger
closed connection is retained in the prior whole27 logs and frozen helper;
that excluded branch was not retried in this NULL stage or investigated.

The independent actual OID-zero neighbour probe retains six missing inferred
ParameterDescription OIDs and two UNKNOWN child Execute errors:
`null-unknown-strict18-v5.log` is 0/all46; both metadata-parent baseline and
NULL candidate are 1/all44/exactly8 unchanged records. Declared OID zero is
not guessed from a NULL datum. Valid custom OIDs also cannot enter the finite
eleven-entry cast map. No full custom-NULL compatibility claim is made: the
old Source84 SQL catalog paths reject pg_type lookup and its real custom
physical Describe publishes OID25, which is not a trustworthy custom OID.
All bounded `null-unmapped-*-v1..v4.log` observations and strict all85 evidence
are retained. They do not justify substituting that wrong TEXT OID, guessing
an OID or changing custom registration in this issue.

The first wire author version compared byte column names with Python strings;
its frozen file/hash and baseline/reference errors are retained. Only that
representation comparison was corrected. Native author v1/v2 falsely compared
raw `INTEGER` CAST spelling with canonical `integer`; the strict wire OID is
23 and the existing canonical type function maps both to the same real type.
Frozen author files and the original 134/complete19-failure logs are retained.
The final native test keeps the identical CASE SQL, NULL/slot/use/effect checks
and strict declared type identity, compared through that existing canonical
type function (a BIGINT/TEXT/UNKNOWN identity still fails). No production
change was made for a nonexistent CASE type-identity bug.
