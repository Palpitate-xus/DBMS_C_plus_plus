# Declared BIT scalar parameter source

## Independent root

Root104's ordinary physical-table protocol path analyzed neither BIT/TEXT
nor BIT/INTEGER scalar parameter operators at Parse. Bind later rendered
actual values into SQL. That rendering is not a declaration of a new UNKNOWN
literal: a genuine declared OID25 or23 parameter must fail `v=$1` at Parse
with42883, whether its eventual Bind value would be non-NULL or NULL. No Bind
value exists yet at Parse, so a value scan/cast cannot repair this identity.

A new finite `validatePreparedBitScalarParameters` bridge uses the original
SQL AST, real `ParameterExpr` occurrences, actual copied physical BIT base
metadata, and declared parameter OIDs. It calls the existing whole-query
binder with typed cells before publishing the statement. It owns scalar
comparisons in a single real table range; complex query hosts retain their
existing owners. This does not substitute SQL, execute a routine, read rows,
guess a dtype from NULL, relabel data, or change non-NULL Bind codecs.

The existing integer input bridge, BIT BETWEEN bridge, ordered occurrence
provenance, real parameter slots, BIT text/binary codec, and all current
Window/enum/CTE/aggregate/signed fields are unchanged. The finite bridge
admits the existing mapped builtin OIDs plus actual BIT1560/VARBIT1562. A
custom OID is left to its existing owner, not mapped by a spelling or value.
OID0 remains unspecified; its inference/ParameterDescription gap is retained
in a separate complete diagnostic, not hidden or claimed repaired here.

When composing onto current Root's explicit ParameterOrigin public ABI, the
new Parse-only carrier is marked MetadataPlaceholder, just like the existing
integer/BIT range metadata carriers. There is no Bind value at this phase;
it must not inherit the default StatementInput value role. This is an
adaptation of the same declaration guard to its actual current receiver,
not a claim that the historical Root104 evidence proves the new ABI.

## Strict reference and complete evidence

Owned PG18.6 reports `server_version_num=180006`. Production artifact root:
`/tmp/dbms-varbit-type-catalog.dDBk0iPF`. Normal build first55271 terminated0,
fresh sole Network CPP plus57 individually source/header/flags/manifest/
original-receipt/cache/byte-proved prefix donors; no public header changes,
no genuinely fresh58 claim. The immutable prefix donor is6dcf5f3a/frozen
`04df6c778effd2facb1b4b14d657a127e3e787c925750277001be3dfceb59763`.
Current guard binary SHA:
`801d41751253ff184ce17b1ab8445851fd92be3051dafe614d8ec30e19babb7b`.

| Complete gate | Actual result |
| --- | --- |
| New scalar parameter protocol strict/candidate |284 controls, both terminal0 |
| Same protocol pre-guard prefix baseline |terminal1,396 records,56 Parse priority differences; extra records reflect wrongly admitted statements |
| New effects protocol strict/candidate |strict32/candidate31, both terminal0; reference-only SET accounts for one |
| Same effects protocol baseline |terminal1,31 records, exactly2 missing Parse42883 |
| Original full actual parameter-source probe Root104 |terminal1,138 records/24 differences |
| Same unchanged full probe after guard |terminal1,123 records/4 differences, solely valid BIT literal-left execution |
| Strict OID0 adjacent probe |terminal0,139 controls |
| Same OID0 full probe candidate |terminal1,139/9; retains four unresolved ParameterDescription OID0 and five literal-left execution differences |
| Final complete native33 |Recorded after actual completion |
| Final complete whole27 |terminal0, all27 suites; effects31 separately complete0 |

Permanent protocol controls retain actual Parse, DescribeS parameter OIDs,
Bind input bytes/NULL length-1, DescribeP INTEGER output OID23, Execute rows,
and Close. All six operators plus `!=`, both literal directions, empty and
populated PK/index tables, and genuine BIT1560/1562/TEXT25/INTEGER23 types
are covered. Those Parse checks cover both parameter operand directions;
the separate BIT literal-left execution root is explicitly retained below.
Ordinary TEXT-column data `'b01'` is not decoded as BIT.

The native parameter test retains all112 whole-binder public-graph controls:
declared dtype independent of value/NULL, exact `$1` source interval/use and
one typed slot, genuine INTEGER physical descriptor, exact non-NULL/empty/
NULL result rows and both operand directions. The existing native prefix
matrix retains all613 assertions with a corrected real INTEGER factory.
The separate effects fixture holds a real transaction across portal Sync,
checks zero writer execution at Parse/Bind/Describe, one demanded execution,
and zero effects for NULL or invalid declared types. Error savepoints retain
the same SQL and strict state rather than shortening the assertion matrix.

## Preserved author diagnostics and open neighbours

Native author v1/v2 selected BIGINT through `makeIntColumn(...,4)` but falsely
expected INTEGER, producing exactly24 descriptor-only differences out of112;
the actual slots/NULL/uses/runtime controls all passed. These source/logs are
frozen. The corrected factory resolves INTEGER through TypeRegistry and
hard-asserts canonical dtype, width4 and fixed length; no semantic assertion
is weakened. The prefix issue document records its original native scale
misstatement separately from independently true INTEGER/OID23 wire evidence.

Effects author v1 ran portal Sync in autocommit, then issued another simple
query before Execute. Both PG and candidate genuinely returned34000 after
that transaction destroyed the portal (complete25/8 each). The original
source/logs remain; v2 establishes the real transaction and error savepoints
needed for the intended phase/no-effects assertion. No deadline was changed.

BIT literal-left frontend ownership is a separate root, retained as four
strong full-probe reds; original VARBIT typed-literal16 remain a separate
parser/catalog issue. Valid custom OID and general OID0 inference are not
claimed closed by this finite declaration guard. No master files, Actions,
donors, filtered view/security branches, or push are touched.
