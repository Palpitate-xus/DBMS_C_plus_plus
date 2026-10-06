# Canonical full run on 1e0a8c5a and independent follow-ups

Session 40539 has finished with exit 1. It ran unchanged
`scripts/build_tests.sh` against frozen source 1e0a8c5a (the later 00ce0209
checkpoint changed documentation only): **501 natives, 496 passed and five
failed; 244 registered protocol/E2E labels, 242 PASSED and two failed**.
The latter count includes an intentional TLS-stub skip, not a TLS runtime
pass. ROOT source, headers, tests and registry stayed frozen until terminal.
The exact frozen binary SHA256 is
`0204a334c1f7b8e7534f8dd07a5e4dffb951ae9441ae211a7e012470865f2728`.
All 56 normal-O2 production units were freshly built and signature-audited;
matching 55 no-main objects and fresh stubs were separately audited. Every
native was freshly compiled/linked and run in isolation. The immutable log is
`/tmp/dbms-prepared-query-namespace-combination.FS4egSBy/full-registered.log`.

## Actual full-gate failures

| Test | Observed result | Current follow-up |
| --- | --- | --- |
| check_add_validation | Omitted nullable CHECK column INSERT returned 22023 | Production omission repair ROOT 1f709b42; private 11 fresh related natives and dedicated wire passed |
| integer_bounds | BIGINT_MIN / -1 overflow raised precise 22003 | Test-only ROOT 4c0f11ee corrects the obsolete empty-sentinel expectation; original ROOT production passed, valid modulo retained |
| geometric | Decimal text was not found in V2 binary SP-GiST file | Test-only ROOT 6efbe274 checks bits/RIDs, corruption fallback and fresh-owner reload; original ROOT production passed |
| partial_index | Function-left predicate raised missing column same / 42703 | Decoded API literal repair ROOT fcdace6b; final private fresh56 O2, 25 natives and six wires passed, existing index assertions preserved |
| temporal_literal_validation | Invalid huge interval input returned SQL NULL rather than a precise range error | Typed-input ROOT 12dc04e4 and independent operator ROOT b4c31a5a; interval storage parser/classification remain separate open work |
| postgres_protocol_test.py | Original line3537 setting assertion failed | Read-only pg_settings transaction/I/O overhead remains open; this full log alone does not establish an exact returned SQLSTATE |
| table_lock_timeout_protocol_e2e_test.py | Expected 55P03; received XX000, could not start statement transaction | Implicit query-owner begin drops its failure status; independent source repair/verification is in progress |

The separate TLS label explicitly reports:
`skipped (binary has no OpenSSL support)`. No timeout, lock assertion,
protocol recovery check or real production failure was removed to make the
gate appear green.

## Independent local ROOT integrations

These are source/test commits, not proof that a later combined build passes.

| ROOT commit | Independent change | Private origin |
| --- | --- | --- |
| 1f709b42 | Nullable INSERT complete final image | 229b2b19 |
| 4c0f11ee | BIGINT overflow fixture, test only | da8bb245 |
| 6efbe274 | Binary geometry fixture, test only | 2575ba98 |
| b74de608 | Native query exception resource guard | 983b38a6 |
| 5daf1859 | Structured CAST error propagation | a95e1076 |
| 12dc04e4 | Typed interval input ranges | b4db9ffa |
| a693d5ea | LIMIT zero execution demand | b52e863b |
| e753b9e7 | Typed aggregate expression roots | 00012a99 |
| 90a570b1 | SUM and interval AVG descriptors | a823d640 |
| c4b7610d | Prepared scalar metadata/binding API | 8eea4e71 |
| 34cac18c | Typed prepared execution carrier | 3e98e86d |
| 6125d08f | WITH scalar query envelope | 14d9cb2e |
| d93dd5c7 | Structured arithmetic errors, single diagnostic suffix | b91e3d06 + 5ddeca6a |
| b4c31a5a | Interval arithmetic field widths | 19f6107e |
| fcdace6b | Native decoded scalar RHS literal provenance | 4a9ea879 |
| 735e1baa | PL/pgSQL RAISE severity and SQLSTATE | 57407b59 |
| 912894e1 | Prepared array element declarations | e00fb9bd |
| efc501b9 | Actual typed EXPLAIN tree and publication | a9e8fc53 |

The shared typed carrier introduces the 57th production translation unit.
The Condition layout and typed-plan public interfaces require a fresh
complete ROOT build, not reuse of the older 56-source/header object layer.
At source revision efc501b9, all 57 production units are being rebuilt at
normal O2 by session 86360. Source, header, tests and registry are frozen
during that build. Artifacts: `/tmp/dbms-native-explain-prepared-combination.64xbTwJu`.
The current source inventory is 516 native tests and 255 registered E2E
entry points; neither count is a claim that they have run or passed.

## Retained red-green evidence and remaining scope

Null omission: changed TableManage at normal O2 plus 55 unchanged formal
ROOT objects, with all 56 signatures, sources, headers and manifest audited;
sessions 47180, 97756 (11 natives) and 18981 (wire) exited 0. Candidate SHA256
`5502bb01babab381c939a1efe86eacc1b06686863b9fdb9a8b10434d9af41992`.
Actual original new regression native 90870 exited 134 and wire 82610
exited 1/22023. Integer 71894 and geometric 88512 corrections exited 0 on
original ROOT production. Initial wrong-RID fixture 85756 exited 134 and
initial live-edited Bash verifier 58134 exited 2; both failures are retained.

API scalar literal baseline 19506 exited 134. Candidate v1 exposed
arithmetic-looking TEXT misclassification; v2 exposed a genuine native query
exception lock leak, independently repaired by b74de608. Final v3 normal-O2
56-source/header signatures and stamp match, native 37922 (25) and wire
23874 (six) exited 0. Exact frozen SHA256:
`8a84a96e98820bf7a5fe5956238997fbd4290e5cca76a014a23971394d3fe25e`.
See `docs/issue-api-scalar-decoded-literal.md`,
`docs/issue-native-query-error-unlock.md`,
`docs/issue-cast-structured-error.md`,
`docs/issue-arithmetic-structured-error.md`,
`docs/issue-interval-arithmetic-range.md`,
`docs/issue-plpgsql-raise-sqlstate.md`,
`docs/issue-prepared-query-array-metadata.md` and
`docs/issue-explain-typed-execution.md` for exact independent proof boundaries.

Typed UPDATE is still in a separate worktree under verification, retaining
the original six predicate/effect controls, an expanded NULL/coercion/
correlation/OLD-row/late-error rollback matrix, 24 native controls and
adjacent wire regressions. Its newer RAISE dependency does not weaken the
original P0001 assertion. Ordinary scalar WHERE/ORDER consumers, distinct
EXPLAIN scalar-child sort slots, table-lock begin status, read-only I/O
overhead, interval storage, and broader query/type/routine families remain
open. Original five-clause diagnostics and earlier full protocol failures
remain attributed to their original frozen revisions.

Totals remain 273: 22 complete, 166 partial, 70 unverified and
15 deferred_by_user. This failed full gate is not relabeled as green, nor
are private proofs relabeled as a new combined ROOT run. No push, no Actions
enablement and no restart of user-deferred security/TDE work.
