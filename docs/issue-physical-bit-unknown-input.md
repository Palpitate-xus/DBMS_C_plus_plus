# Physical BIT scalar UNKNOWN input

## Root cause and bounded scope

The original unchanged 384-case PG18.6 comparison had 23 differences on
Root source104 (`168596fce4a99c63e0bd7c6f40bd52c9ec434c0c`). Seven are
ordinary physical BIT/VARBIT scalar predicates such as `v='b01'`,
`v='B001'`, `v='b'`, and `b='x1'`. The input was admitted with BIT
semantics, but the compact receiver and index key still used the raw text
prefix instead of the actual BIT input datum. The other 16 are a separate
VARBIT typed-literal parser/catalog-identity issue and remain open here.

`parseConditions` now marks only SQL quoted string input as UNKNOWN; native
`apicond` decoded data retains its existing identity. Physical schema-owned
scalar comparisons prepare a local copy using the real comparison resolver
and coercion codec. Actual `bit` / `bit varying` storage base types, not a
column/type spelling, select that input function. A TEXT column or a TEXT
domain called varbit is not decoded as BIT. Comparison conversion has no
column typmod: BIT(4) compared to `'b01'` must not pad that input to `0100`.

Both TableManage compact receivers and the public planner prepare input
before row execution, index selection, OR branches, or a zero-row demand.
Index keys, residual predicates, and filter constructors consume the same
canonical datum. Caller-owned conditions remain unchanged. This does not
evaluate a source row, parameter, function, UDF, or volatile expression to
discover metadata. There is no SQL-wide string replacement or public header
change.

## Evidence and reproduction

Artifacts are owned under `/tmp/dbms-varbit-literal-owner.gfvbZ7IT`.
The immutable original driver is
`/tmp/dbms-bit-text-input.MoHEgLcd/probe_bit_text_input.py`; its original
SQL, assertion set, disk placement, 15-second wire deadline, and 20-second
startup deadline were not changed.

| Gate | Actual result |
| --- | --- |
| Strict owned reference | PG `server_version_num=180006` |
| Original Root104 baseline384 | terminal1, all384, exactly23 differences |
| Compact candidate original384 | terminal1, all384, exactly16 typed-literal differences; all7 physical prefix differences removed |
| New protocol v1 baseline | terminal1, all403,108 failed records |
| New protocol v1 strict/candidate | strict404/candidate403, both terminal0 |
| Strong final protocol strict | terminal0, all432; includes strict-only SET LOCAL |
| Strong final protocol candidate | terminal0, all431 |
| Strong final protocol Root104 baseline | terminal1, all431,111 failed records |
| Public planner pre-fix native | terminal1, all580,106 remaining planner differences |
| Strong final native baseline | terminal1, all613,531 failed controls |
| Strong final native candidate | terminal0, all613 |
| Final full native32 | terminal0, complete32, zero failed suites |
| Final whole26 | terminal0, complete26, zero failed suites |

The permanent native test checks all six comparison operators plus `!=`,
leading zeros, empty and hex/binary prefixes, NULL exclusions, invalid input
before empty/populated rows, unchanged caller data, real public planner
execution, actual secondary/primary IndexScan nodes, indexed OR, and BIT(4)
unconstrained input. The permanent protocol test also retains exact OID23
results, real quoted physical names with spaces/embedded quotes, a genuine
schema-qualified varbit AS TEXT domain, LIMIT0/false/OR admission, and a
writing projection's once-only effect with no effect for bad input.

Production verification is normal O2: first fresh TableManage CPP plus57
byte-proved Root104 donors, then fresh ExecutionPlan CPP. The helper proves
every original donor source/header/flags/manifest, all58 original genuine
fresh compilation receipts, donor cache/frozen bytes, current per-object
receipts, repeat build, and current frozen binary. This is **not** another
genuinely fresh58 build. Native drivers and stubs are freshly compiled and
run in owned default-disk working directories. Root104's frozen donor is
unchanged; its binary SHA is
`3d6446bee02de0ac8ec976cc157b17caf368de452f17891ede6db4eef4f0fc9c`.

Current production freeze SHA:
`04df6c778effd2facb1b4b14d657a127e3e787c925750277001be3dfceb59763`.
The exact adjacent native32 and whole26 file lists are in
`verify-varbit-current.sh` (`native` / `wire`), with unchanged deadlines and
no temporary-memory relocation. Original16 type-literal reds, declared TEXT
parameter Parse identity, and BIT literal-left frontend ownership are
separate issues; this is not a claim that those families are complete.

## Author controls retained

The first new native author version incorrectly called private `filterRows`.
It failed compilation and is frozen as `bit_scalar_unknown_input_test.author-v1.cpp`
with `bit-scalar-native-first.log`. The corrected test uses the actual public
planner/index node and preserved all semantic checks. A later author version
omitted `makeIntColumn`'s required width argument; its compile failure and
source are retained in v3 artifacts. That was corrected to the genuine
required scale argument4, not a production change or a weakened assertion. An initial
manual baseline invocation used zsh on bash build helpers and failed; the
proper bash rerun uses genuine frozen donor objects and fresh driver/stub.
The full baseline/candidate controls, including failures, remain available.

## Followup native-factory correction

The preceding author explanation incorrectly called that API argument an
INTEGER byte width. `makeIntColumn` takes a scale enum:2 means INT/4 bytes,
whereas4 selects BIGINT/8 bytes. The frozen `6dcf5f3a` native613 comparison,
NULL, index, OR, and unchanged-input controls remain genuine, but their `id`
fixture was BIGINT. Its independent strict432/candidate431 protocol fixture
uses actual SQL `INTEGER`, and verifies OID23; that claim is unchanged.

This followup changes only the new native test factory: the real central
TypeRegistry resolves INTEGER and hard-asserts canonical `integer`, width4,
and fixed-length metadata for the actual physical columns. All original613
semantic controls remain unchanged. The new parameter native fixture uses
the same genuine INTEGER factory and retains all112 type/NULL/uses/runtime
controls. No production dtype or assertion expectation was relaxed. The
earlier parameter v1/v2 complete112/24 failures came solely from that author
fixture's actual BIGINT descriptor, not from a production INTEGER defect;
both versions and logs are frozen in the followup artifact directory.
