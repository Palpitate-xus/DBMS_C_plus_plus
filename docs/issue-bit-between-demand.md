# BETWEEN logical demand and independent left-expression sites

This is a finite TYPE-11 follow-up to `f5d1c29d92817438210f337da43c71b100d61884`.
It fixes the six retained real BETWEEN writer differences. It does **not**
complete TYPE-11, the original 273 controls, or the entire review checklist.
The private tree is `/tmp/dbms-bit-between-demand.l1jfNbiP/repo`.

## Actual defect and SQL semantics

The old evaluator evaluated all three function-shaped grammar operands eagerly
and reused the left datum. PostgreSQL lowers BETWEEN into two comparisons:
`lhs >= lower AND lhs <= upper`; NOT BETWEEN uses `< OR >`. A demanded second
comparison is a second occurrence of the left expression. It is not an
always-once construct. Real volatile writers therefore run twice, once, or not
at all depending on logical demand and constant NULL planning.

The immutable original writer 12 matrix had six differences after the input
fix. A new complete 128-case matrix had 68 differences. Actual quoted and
schema-qualified three-argument user functions named `between`/`not between`
had six wrapped-expression differences in 24 cases. Four of 12 scalar-child
cases remained wrong after the first demand change: memoization by the single
original expression pointer merged the two demanded scalar SQL occurrences.

An initial NULL/short-circuit candidate also introduced six errors in a new
15-case constant-error matrix by suppressing direct primitive CAST input errors
or pure arithmetic errors under a strict NULL comparison. These were retained
as failures until repaired; they were never reported as PASS. Two preexisting
BIT(0) typmod errors remain in that complete original matrix.

## Finite implementation

- Evaluate each demanded comparison separately; use SQL three-valued AND/OR.
  A genuine constant NULL can eliminate its strict comparison's volatile peer.
  A row/provider returning NULL is not a constant. Bound parameter cells have
  explicit typed NULL flags and require no provider/expression execution.
- Preserve pure UNKNOWN-string CAST input conversion before logical pruning.
  The new `between_input.h` admits a limited structural primitive shape only:
  no quoted/qualified/modded/custom spelling, parameter, routine or SQL child
  is executed by its pure primitive validator. It is **not** a general catalog
  owner/type-resolution proof; bare alias/domain lookup remains open below.
- Copy a fourth, execution-private left occurrence for parser BETWEEN grammar.
  Its real prepared scalar children retain original statement/binding metadata
  but get independent child/memo keys. The shared prepared SQL AST is unchanged.
  New child sites erase a possible old memo cell before use.
- At the actual `planStatementConstants` boundary, fold demanded pure pairs in
  order. Pure peers retain arithmetic errors before strict NULL elimination;
  a decisive first constant pair leaves an unreachable upper arithmetic/routine
  argument/SQL child unplanned. Planning never invokes a stored writer.
- Preserve actual user routine identity. The 24 native and wire role cases use
  real stored functions, including quoted unqualified, schema-qualified and
  quoted-schema-qualified names, not synthetic callback substitutes.

Only four CPP files and the new private structural helper header change
production code. No existing public class/layout, Main, Network, Actions or
remote Git configuration is changed. Future Root integration must retain its
newer enum/type ownership, integer input, Boolean BETWEEN metadata, window
fields and expression-copy fields; these older private objects are not donors
for the current Root ABI.

## Permanent controls

`tests/bit_between_demand_test.cpp` reaches all 197 raw prepared-cursor cases,
13 actual owned constant-planning cursor cases, 165 real helper cases and 36
native populated/empty/row-NULL/cap0 projection cases. Its final 587 assertions
include pre-body preparation checks and typed values/descriptors. The explicit
13 planning cases use the real prepared execution and child cursor factory;
they do not pretend the raw cursor API's `planRootConstants=false` mode performs
the SQL host's optional planning stage. Native typed cells carry canonical BIT
datums; real raw `b01` input is separately tested through actual wire Bind.

`tests/bit_between_demand_protocol_e2e_test.py` contains the complete 128 demand,
24 real routine-role, 12 real scalar-child, 13 constant-error and 13 planning-
demand controls: 190 total. It checks rows, NULL, SQLSTATE, names, tags, OIDs,
sequence effects and absence of RowDescription/DataRow on analysis/planning
errors. Error rollback cannot hide side effects: currval is checked separately.

`tests/bit_between_demand_parameter_protocol_e2e_test.py` contains four real
statements times five raw values: 20 controls. It sends real Parse/Bind/
statement Describe/portal Describe/Execute/Sync, verifies parameter OID 1562 and
Boolean descriptor `(16,1,-1,0)`, NULL/custom-plan demand, input errors and zero
writer effects during Parse/Describe. Both fixtures support strict PG 180006
reference execution and are registered in `scripts/build_common.sh`.

## Actual terminal evidence

The frozen functional binary is
`/tmp/dbms-bit-between-demand.l1jfNbiP/dbms_main.between-demand-v6.frozen`, SHA256
`162d75e15982d38ab5c85d4d75374e60551a961699f7ffff19b518f1cfb6b521`.
All logs below are in `/tmp/dbms-bit-between-demand.l1jfNbiP`.

| Gate | Actual terminal result | Evidence |
| --- | --- | --- |
| New-header production build, repeat, all58 receipts/stamp | session 15095 = 0; genuinely fresh all58, no old-header donors | `between-demand-normal-v4-fresh58.log` |
| Subsequent current-header changes, normal/repeat/receipts | session 81762 = 0 | `between-demand-normal-native-v6.log` |
| New full native 587 controls | session 90909 = 0 | `between-demand-new-native-final-v6.log` |
| Original 23 plus 8 adjacent native fixtures | session 33664 = 0, all 31 complete | `between-demand-final-normal-native-v6.log` |
| 24 complete wire fixtures and 3 independent CLI processes | session 51032 = 0 | `between-demand-final-v6-whole-wrapper.log` and its per-file logs |
| 17 complete strict PG18.6/180006 fixtures | session 20996 = 0 | `between-demand-final-v6-strict-wrapper.log` and its per-file logs |
| All 12 unchanged original diagnostic matrices collected | session 73842 = 0 as a retained-status audit, not family PASS | `between-demand-final-v6-original-wrapper.log` |

The v4 binary was `0626e1f0c9ed32b65494ecf0d07bdbb6ee058b176b4c1b81075612a5f333925c`.
Subsequent changed CPP objects were rebuilt against the same new header, not
old f5 headers. Every current receipt and repeat/stamp was verified. The v5
compiler failure (member/private comparison API misuse) is retained in
`between-demand-normal-native-v5.log`; it is not a success receipt.

Original full matrix results are 132 actual1/3, 384 actual1/33, 1296 actual1/26,
250 actual0, 40 actual0, 50 actual0, writer12 actual0, demand128 actual0,
roles24 actual0, child12 actual0, constant15 actual1/2, planning13 actual0.
Their exact SQL, assertions, timeout and reference version checks were not
changed. The status-audit wrapper's zero means it collected those precise
retained failures, not that all underlying matrices passed.

## Open work and owner boundary

The original 132 retains three INTEGER empty-string comparison input failures
on this older private source. Root's independently fixed integer source is not
present here. The original 384 retains 33 differences, and 1296 retains 26
non-BIT INTEGER UNKNOWN conversion differences. No new red index appears in
these complete matrices versus frozen f5; exact SQL agrees except owned unique
schema names. The original writer's six red indices 0/2/3/5/6/8 are resolved.

The two constant15 reds are exactly:
`NULL::bit(0) BETWEEN B'0' AND B'1'` and
`NULL::bit BETWEEN NULL::bit(0) AND B'1'`: PG22023, private Boolean NULL.
They remain typmod work, not demand successes.

A new complete eight-case owner probe creates a uniquely owned domain named
`varbit AS text`, then uses `search_path=owned_schema,pg_catalog`. Both f5 and
this candidate have the same seven failures, no introduced difference:
bare CAST/NULL varbit aliases incorrectly take builtin BIT input; typed VARBIT,
quoted VARBIT and BIT VARYING literal forms still fail 42703. Its logs are
`between-domain-original8-baseline-f5.log` and
`between-domain-original8-candidate-v6.log`. The raw-name structural whitelist
must not be cited as actual catalog ownership for these aliases, and Root's
future type-literal/domain-owner integration needs separate owner validation.
This finite commit does not approve that alias/domain family or numeric casts,
operators/typmods, descriptors, binary padding, or the original 273 controls.
