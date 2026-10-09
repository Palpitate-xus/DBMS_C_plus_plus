# Interval field declarations and actual syntax-diagnostic ownership

Six scoped source repairs from the HAVING/lifetime/LIMIT/interval follow-up
are now independently published on master (180 scoped repairs in total).
Current inventory:766 automatic native tests +2 actual Main drivers,
430 registered protocol/E2E entries,58 production compilation units.
Neither these counts nor a focused passing matrix prove the273 requirements.

## Independent Git provenance

| Private commit | Master commit | Scope |
| --- | --- | --- |
| `48d35f1d` | `7fe32138` | Genuine HAVING AND keyword boundary. |
| `6968d94d` | `f78853a9` | Genuine grouped-column identity, not parentheses in names/literals. |
| `cf4e62a0` | `0313e441` | Immutable type grammar dictionaries during engine shutdown. |
| `e5114a10` | `7cb068a8` | Original invalid LIMIT negative plus ordered FETCH positive; test-only. |
| `75480b5e` | `16823731` | Ordinary/Extended invalid LIMIT error handoff. |
| `893f628f` | `26d981dd` | Keep live audit links out of evergreen README; test-only. |
| `9dd1011f` | `f88b4971` | Canonical TIMETZ expectation, same original SQL; test-only. |
| `0e961c5f` | `bb62238e` | All13 interval field grammar forms and exact explicit-cast value truncation/precision. |
| `bcf0a4ce` | `974b383f` | Preserve actual42601 parser diagnostics before ordinary/Extended legacy fallbacks. |
| `922fed63` | `b70710ab` | Same original hidden-key FETCH SQL, exact positive row/type/tag instead of stale0A000 assertion. |
| `1c33403d` | `1e773568` | Same original SQL-function reader SQL, exact row/type/tag instead of stale error assertion. |

Each import uses `git cherry-pick -x`. Only verified source candidates were
imported, after the previous whole Root driver86574 terminated1. No push.
The two final fixture corrections also ran their entire original protocol
scripts successfully (15407/66786), not just the modified assertion.

## PostgreSQL grammar and actual oracle

The field phrases and precision grammar are checked against
[PostgreSQL18 datetime documentation](https://www.postgresql.org/docs/18/datatype-datetime.html)
and the primary
[parser grammar](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/parser/gram.y).
The semantic field-mask layout and least-significant-field truncation follow
the primary
[interval typmod implementation](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/adt/timestamp.c)
and [field definitions](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/utils/datetime.h).
Behavior is verified on the explicitly selected real PostgreSQL18.6 server
(`180006`), not inferred from documentation or an older reference server.

The parser retains semantic field masks, then renders genuine SQL before
another binder consumes a type declaration. TypeRegistry records the packed
interval modifier. Explicit casts retain higher-order fields, truncate lower
fields and round fractional seconds using checked128-bit intermediate
arithmetic; bare numeric input uses the declaration's least field. NULL and
infinity remain typed interval values. No SQL text substitution or runtime
expression evaluation is used to validate types.

An explicit42601 diagnostic now stops ordinary execution/Extended publication.
Unsupported parser shapes without that diagnostic retain their existing
analyzer ownership. Native FLOAT/declaration, whole default protocol and
original query/routine fixtures are included in the verification matrix.

## Actual verification and retained failures

Artifact directory: `/tmp/dbms-having-grammar.MPk64yc0`.

| Invocation / evidence | Actual result |
| --- | --- |
| New interval protocol against actual180006 | Exit0;49 exact SQL/row/OID/SQLSTATE controls. |
| Previous frozen interval baseline54598 | Exit1;49 controls,30 failures. It silently ignores fields/precision and accepts invalid grammar. Original results/log retained. |
| Actual private fresh57/Main64854/54367 | Both exit0;57+1 genuine compilation entries, own-path objects and source/header/compiler signatures. New public headers require all58 units; no foreign objects copied. |
| Initial interval gates54220 | Exit0;41 native and36 wire entries all pass, including new95 native assertions, original pattern-cast fixture, full original interval136 and default protocol, recursive CTE, scalar FETCH92 and original HAVING matrices. |
| Original BIT38418733 | Exit0 against real180006;384 cases,zero differences. |
| Original hidden-FETCH and routine reader reference | Real PostgreSQL18.6 gives exact one-row/OID23 outcomes. Prior candidate gives the same; the separate zero-arity probe returnsXX000 rather than reference42883 and remains a genuine open bug. |
| Complete corrected set-operation fixture15407 | Exit0;all remaining assertions/SQL unchanged. |
| Complete corrected function-result fixture66786 | Exit0;all remaining assertions/SQL unchanged, including original negative arity assertion. This does not prove correct42883 for zero-arity calls. |
| New five-control interval storage/literal reference | Exit0 against180006. |
| Exact same five-control candidate probe | Exit1;2 failures: column assignment loses precision, postfix typed literal ignores DAY. Valid `INTERVAL(3) '…'` already matches. Both genuine failures retained for next repairs. |

Initial interval frozen binary:
`dbms_main.interval-fields-initial.frozen`, SHA256
`2acba11d424beed6dbc1302d1cabaf0341ce81fff3f807426cb090612a2efbed`.
Gate source seal:
`bf2dc096114e83c5934a44a5bed777d53cabd5abcf3e7b05045e2c983445f207`.
Current58 own receipts/cache/no-recompile repeat/source/frozen fences pass.
The two later test-only corrections change the source seal but not production
bytes; no prior frozen binary or failed generation was overwritten.

## Actual Root publication proof

Root and the final candidate `src/scripts/tests/cmake` bytes match exactly:
`f048f155e3008b6ffd556f5780e4ad72491890a5bf27833af241cb9b74309e44`.
Root artifacts: `/tmp/dbms-root-interval-fields.M3CeQmTN`.
Actual Root fresh57 build42176 and Main1612 both terminated0, with57+1 actual
compilation entries. Actual Root gates8020 terminated0:41 native and38 wire
entries all pass, including both complete corrected original fixtures.
All58 current own-path receipts/cache/no-recompile repeat/source/frozen fences
pass. Actual Root unchanged BIT38472314 also terminated0 against180006.
These are genuine Root results, not inherited private green results.

Root frozen binary: `dbms_main.root-interval-fields-final.frozen`, SHA256
`2acba11d424beed6dbc1302d1cabaf0341ce81fff3f807426cb090612a2efbed`.
It equals the candidate production binary, after genuine Root compilation.
The unchanged current full766 automatic+2 Main/430 protocol driver88630 has
started and is confirmed live, building its own Root-path standard test
objects. Root inputs remain frozen until its actual terminal result.

The previous unchanged full766/427 run is terminal1 with6 native/Main and
11 protocol failures; its evidence is in
[the full-run record](issue-full-registered-b2b376a3.md).
It is not replaced by the passing focused matrix. No completed full current
inventory, sanitizer or real-TLS runtime proof is claimed.

## Required next steps

1. Monitor exact current full driver88630 to its terminal result; retain every
   original failed entry. The41/38/BIT384 Root publication gates are complete.
2. Fix and verify real interval assignment/storage modifier persistence and
   postfix interval typed literals, keeping the five actual-reference SQL
   probes unchanged. Broader type IO/array/binary/notice behavior remains open.
3. Independently fix ordinary scalar child aggregate lowering, routine arity
   diagnostics and SQL-function session-value binding; retain all original
   whole-run failures and user-filtered/deferred branches.
4. Continue every original in-scope273 requirement, without marking whole
   TYPE-06/query/routine families complete from these focused controls.

Original ledger remains22 complete/166 partial/70 unverified/15 user-deferred;
item entries are unchanged. README is evergreen in aed6e80a, Actions disabled.
