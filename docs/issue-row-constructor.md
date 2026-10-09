# Structured ROW constructors and typed record datums

ROW was previously parsed as an ordinary scalar callee and rejected with
42883. The strict global-aggregate entry retained MIN(ROW(1,'a')) as its last
failure after the independently committed numeric column repair.

Explicit unqualified/unquoted ROW and implicit comma-row parentheses now
have a constructor role distinct from the existing scalar IN-list container.
The binder preserves each field's declared type, including contextual UNKNOWN.
Runtime records carry actual typed fields and their NULL bits separately from
PostgreSQL record text output. Output quoting preserves empty versus NULL,
commas, double quotes, backslashes, whitespace and nested records/arrays.

SQL constructor comparisons retain three-valued field equality, lexical
ordering and actual short-circuit evaluation. Row IN peers have distinct
comparison signatures and reached left occurrences, not a cached formatted
tuple. Pure constant false equality fields prune unreachable peers without
invoking volatile functions; immutable errors still arise during root planning.
Record datums used by MIN/MAX have the separate record ordering semantics:
NULL fields sort high, and reached UNKNOWN fields (including NULL) lack a comparison
implementation and raise 42883. This is not textual record sorting.

Execution copies preserve constructor roles, field signatures and graph
ownership. Native scalar preparation retains the same row comparison metadata.
The source adapter now consumes actual lastStructuredValues instead of
reconstructing records or OID identities from display strings. Genuine top-level
constructor expressions enter the existing prepared root/constant-planning
consumer. Original SQL, shared AST and parameter frame remain unchanged.

Record's fixed 2249 identity and its 2287 pseudo-array catalog identity are
published from builtin descriptors, with the observed p/P category, alignment,
length and storage properties. This is not completion of all pg_type routine
columns, anonymous-record array codecs, named-composite storage/binary protocols,
record row-SubLinks, grouping/hash semantics or the entire ROW/composite family.

## Actual evidence and failed generations

Artifacts: /tmp/dbms-having-grammar.MPk64yc0.
Reference is explicitly PostgreSQL 18.6, server 180006.

| Evidence | Actual result |
| --- | --- |
| New reference protocol | 30 controls including BEGIN, zero failures, exit 0. |
| Previous numeric frozen baseline | 33471 exits 1: 29 controls, 22 failures; original log/binary retained. |
| Demand reference probe | First probe aborted its transaction and yielded 25P02; retained, not used as downstream evidence. Corrected per-query-savepoint probe confirms field demand, NULL behavior and reached occurrences. Extended probes verify zero-row comparison 0A000 and immutable division 22012. |
| Initial own production compile | 87825/16571 both exit 0: fresh own 57 plus actual Main after public header changes. |
| First direct native group | 43135 exits 1: 4/5 pass, new CASE record identity 42704 retained. |
| Native scalar diagnosis | 19518 exits 0 and records genuine wrong non-NULL equality/IN results for nullable ROW fields. |
| Actual catalog/native corrections | 58089/89059/72228 all exit 0; record identity and native comparison metadata repaired. |
| Initial complete focused gate | 49242 exits 1: 16/17 native and 13/14 protocol entries pass, including entire default protocol and all strict global aggregate 55 controls. New native exposes lost record fields through VALUES; new protocol retains three root demand/planning failures. |
| Typed-carrier/Main corrections | 56338/67953 both exit 0. |
| Corrected direct native group | 6372 exits 0: 5/5 entries, new actual graph/typed-fields/repeated execution fixture has 29 controls. |
| Corrected expanded gate | 27405 exits 1: 23/24 native and 14/16 protocol entries pass, including the complete original default protocol. New ROW29 and strict aggregate55 all pass. Original physical-catalog fixture retains one actual pseudo-array backlink inconsistency; two original composite NOT IN fixtures retain a root-admission regression. All 58 current own receipts/cache/repeat/source/frozen terminal fences pass. |
| Strengthened reference labels | Original 30 controls now also check constructor/implicit-row/CASE/subquery output names; all pass on verified180006. |
| Bare evaluator diagnosis | 17371 exits 0 and records wrong non-NULL row equality/IN through the actual parser and direct evaluator. The logs are retained; neither helper nor query binding is substituted for this API check. |

Initial complete frozen binary: dbms_main.row-constructor-initial.frozen.
SHA256: 87b9c3b81546655ba261f17c79d89ce70badf54a7ed12ffda257881359f8b012.
Source seal: 3817de8215caff66d388d5ddaf6537239ed8fcc64c9e151e457e9aaee849e330.
All 58 current own receipts/cache/repeat/source/frozen terminal fences pass
for the failed complete generation. No foreign object, relaxed assertion,
changed original input or test deadline is used.

Corrected expanded frozen binary: dbms_main.row-constructor-corrected.frozen.
SHA256: 08c5dbc9c59d8de204f3e55f794095b9dc701623e39bae206c0acbdc0ae50f7a.
Source seal: 62a4126acc9ccf28efa756fcc2b3a595c628bfd880d2fc002629b57f710b5e19.

After terminal, actual scalar/array identity guards now publish the record
pseudo-array backlink without falsely changing its PostgreSQL P category to A.
Constructor binding retains fixed record OID2249. Row IN query grammar retains
its Subquery role and the existing relational row-IN consumer rather than
being assigned an unsupported scalar child role by new constructor admission.
Raw native evaluator calls prepare field signatures from actual AST/context
type metadata; they never infer tuple types from formatted values. Existing
binder-owned row signatures win over secondary scalar preparation.

The final expanded 37985 gate exits 0: all 24-native/16-protocol entries pass,
including entire original composite NOT IN and default protocol entries. All58
own receipts/cache/repeat/source/frozen terminal fences pass. Frozen binary
dbms_main.row-constructor-final.frozen has SHA256
49400723dbd788327a8b698c735c392e479deeac90abc183375c480025a20d47,
source seal d8c1491fea956b38806fe352e08f81d007e5aecc75efdfb2e35d48b50f495f18.

Further actual PostgreSQL18 probes expose reached UNKNOWN NULL record-field
comparison and anonymous record cast/input admission gaps in that generation.
The retained artifact-only candidate logs demonstrate wrong acceptance without
changing its frozen inputs or erasing its passing narrower proof. Reached field
operator lookup now precedes field NULL handling, still preserving an earlier
unequal prefix's lexical exit. Pure cast admission rejects incompatible types
even over empty input. UNKNOWN string literals invoke anonymous input admission
at binding; typed text input raises 0A000 only when reached. NULL input and genuine
record-to-record typed fields survive. String output casts remain admitted.
The strengthened registered reference exits0 with53 controls including BEGIN;
own fresh57/Main17874/68356 both exit0 and native58041 exits0 with5/5 entries,
new native48 controls. Expanded93661 exits1:24/24 native and15/16 protocol pass;
new protocol52 controls retains one actual typed TEXT::record WHERE false
frontend error. Entire default protocol and original BIT384/43083 both pass.
All58 current receipts/cache/repeat/source/frozen terminal fences pass.
Failed frozen dbms_main.row-record-admission.frozen SHA256:
2b1ba78136bc31cc76ebe7d54c31e3af1c15e5a2f3431956223faf71eacdf801;
source seal7f1884cc8bb213838fa7eced4d455d93d9d42759fd12cc013df18e452b0ce838.

The existing native prepared query consumer correctly skips typed string input
over WHERE false. Main's legacy scalar entry instead performs premature input
preparation. Record CAST/:: targets now request the same retained prepared root,
using the existing pure declared-type admission guard. The original failing SQL,
expected zero rows/type/name and unchanged deadlines remain in the full group.
Actual affected Main75496 exits0, but expanded11026 exits1:24native all0 and
15/16protocol pass; both CAST/:: typed-string WHERE false controls remain wrong.
Native50 controls passed because the ordinary buildPreparedQueryPlan API does
not enable root constant planning; Main's actual root does. The passing native
entry is not evidence that this earlier diagnosis resolved the whole path.
All58 receipts/cache/repeat/source/frozen terminal fences pass. Frozen
dbms_main.row-record-final.frozen SHA256:
5fc4a441bed5e2a55a9eab40d49d4b7cda33c7ce7762f6d1a59e12edf9666a50,
seal14890495fbed1a8f198876c13f9f550678855b441e42356e4f8d33247d502d5d.

Verified180006 pg_proc explicitly reports record_in/record_out STABLE (s),
textin/textout IMMUTABLE (i). The actual root simplifier was incorrectly folding
all constant casts as immutable. Anonymous record input casts now simplify
their operands without executing record input at planning. UNKNOWN input still
fails at binding; reached typed input still fails at runtime; WHERE false skips
it. The native entry adds actual planStatementConstants checks, and the complete
group adds both original prepared_constant_planning and prepared_query_execution
entries (26native/16protocol). New registered reference55 controls all pass.
Affected CPP52901 exits0. Final complete expanded49135 exits0:26/26 native and
16/16 protocol entries pass, including both original planning fixtures,
original composite NOT IN entries and entire default protocol. The native ROW
entry now has52 controls; registered wire54 and verified180006 reference55
(including BEGIN) all pass. Original BIT384/7762 exits0 against verified180006.
All58 current own production receipts/cache/repeat/source/frozen terminal fences
pass. No foreign objects, relaxed SQL/assertions or deadline changes are used.

Approved final private frozen: dbms_main.row-record-stable.frozen.
SHA256:02c69e429bedb08cbfba0bc35536c54c9c7578f2e616cb18475d4c85a64669fe.
Source seal:795479c69ecc0f8429013e7c4706858e59fbfe3659a1aa503824dbfd09ef393d.
The independent source repair is ready for its own local commit. Master must
import with provenance and build its own actual58 generation before claiming
Root publication. Earlier failed generations remain retained. No whole-suite
pass or original family-completion claim is made.

Master's preceding whole dc89 driver is terminal with all 12 failures retained.
Three earlier independent source repairs have been imported, and actual
Root-owned numeric publication proof has reached terminal with its first
numeric timeout and strict ROW dependency retained, plus an unchanged full
numeric-entry repeat and original BIT384 both passing. No Root ROW import,
whole-suite pass/family completion claim, push or Actions activation.
