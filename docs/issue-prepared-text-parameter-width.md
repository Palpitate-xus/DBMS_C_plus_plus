# Declared TEXT parameters have a variable-width protocol descriptor

Status: independently repaired in the private Source84/signed-FETCH epoch.
This is not a claim about Root's later Source99 composition or every FETCH
consumer. No master changes, push, Actions, or public-header changes were made.

## Exact defect and change

A genuine extended Parse with OID 25, followed by statement/portal Describe,
published TEXT `type_size = 0` for `SELECT $1 AS value` and `VALUES($1)`.
PostgreSQL 18.6 publishes `-1`, independently of NULL, empty text, the string
`NULL`, embedded quotes/parameter-looking bytes, and wire format.

`describePreparedResult` correctly retained the actual parameter OID, but
`protocolTypeSize(25, Column{})` fell through to the default physical column's
zero `dsize`. TEXT has a known builtin variable-width identity; adding exactly
`case 25: return -1` fixes this receiver. No parameter value is inspected,
decoded, executed, or reclassified. Custom OIDs, typmods, relation provenance,
non-TEXT widths, and execution SQL are untouched.

The permanent `prepared_text_parameter_width_protocol_e2e_test.py` checks
genuine named Parse/ParameterDescription/Bind/Describe/Execute/Close cycles.
Both text and binary parameter/result formats are tested for NULL, empty text,
`NULL` text and `O'Brien $1 NULL`, for both real SELECT and VALUES receivers.
Every descriptor asserts name, zero physical origin, OID, width, typmod and
format; execution asserts actual values/NULL flags and `SELECT 1` tags.
Explicit savepoints collect all failures without aborted-transaction cascades.
The normal e2e list registers this test; original fixtures are unchanged.

## Immutable evidence

Artifact root: `/tmp/dbms-typed-null-bind.lMtqOUwj`.
Base commit: `4757cc3a2cbccb6fb535d0a975e55c8d858c89dd`.
Owned reference: PostgreSQL version `180006`, C/libc locale, port 15486.
Passwords are not present in these logs.

| Gate | Actual terminal result |
| --- | --- |
| `text-width-strict18-v1.log` | 0; all 113 controls, no failures |
| `text-width-baseline-v1.log` | 1; all 113 controls, exactly 32 wrong-width records |
| `text-width-normal-v1.log` | 0; one fresh production Network translation unit, then repeat cache check |
| `text-width-candidate-v1.log` | 0; all 113 controls, no failures |
| `text-width-native19-v1.log` | 0; all 19 original native drivers, fresh drivers/stub, unique owned disk cwd |
| `text-width-whole27-v1.log` | 1; all 27 drivers attempted, four unchanged/open suites below |

Frozen metadata-only binary: `dbms_main.text-width.frozen`, SHA-256
`91a800ff915293ff8c81673bc30b426c4f5c256e0f4436bf06ea6a941b2a0bdb`.
`verify-width-v1-frozen.sh` records the exact execution lists and proof.

Before reusing objects, the helper proves all 58 donor source/header/flags/TLS/
manifest/object receipts/cache signature/frozen binary bytes individually.
Donor is the clean, frozen signed-FETCH tree at the exact base commit above,
binary SHA-256
`d3d54e73f7f6b3eac1cacf1dfb301931f613a9910ada38433b08577627e4c3bb`.
This stage reuses 57 byte-proven compatible objects and genuinely compiles
NetworkServer.cpp; it does not claim a new genuinely-fresh-58 build. The donor
signed-AST epoch had its own genuine all-58 compilation. All headers and other
production sources here remain byte-identical to that donor.

## Open neighbours are retained, not counted as passing

The new typed-NULL matrix's original 58 records become exactly 44 real
scalar-child `XX000` execution errors once the 14 TEXT-width records disappear.
Its declared-OID/NULL execution issue is a separate next commit, not this fix.
The original scalar FETCH matrix still has all 18 nonnegative unrelated reds.
Original subquery DISTINCT expects `0A000` but candidate/reference return
`21000`; its assertion is not changed. Original view-trigger values ended with
a closed connection after its original `23514` control, both before and after
this change. That branch is retained as old red evidence and is not retried or
diagnosed further. Defaults, deadlines, original SQL and assertions remain
unchanged. A finite TEXT-width fix therefore does not make the whole run green.
