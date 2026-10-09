# Interval literal and durable column-modifier follow-up

This is a scoped continuation, not completion of the original 273-item audit.
The original ledger remains 22 complete, 166 partial, 70 unverified and
15 user-deferred. No push; GitHub Actions remain disabled.

## Plan and current state

1. Keep README evergreen and commit it independently: done on master in
   `cfcab3cd`. Documentation-status and compatibility-contract checks pass.
2. Fix postfix interval fields on actual typed constants: committed privately
   as `c9a25ab6`, not imported while the Root whole-suite inputs are frozen.
3. Retain interval declaration modifiers through column assignment, schema
   persistence, ALTER TYPE and physical-column protocol descriptions:
   committed privately as `aee0b1bf` after the final matrix terminated.
   All 46 native and 39/40 protocol entries pass; original TEMP CTAS timeout
   remains a failure, not a whole-green result.
4. After the current Root full driver terminates, import only verified repairs
   individually with Git provenance and verify actual Root-path build objects.
5. Continue ordinary scalar aggregate children, SQL-function arity and session
   value binding, then the other in-scope original checklist requirements.
   Do not reopen user-skipped security/TDE or filtered investigation branches.

## Literal repair: actual evidence

Private worktree: `/tmp/dbms-having-grammar.MPk64yc0/repo`.
Artifacts: `/tmp/dbms-having-grammar.MPk64yc0`.

- Explicit PostgreSQL 18.6 (`180006`) reference: all 30 literal controls pass.
- Same 30 controls against the previous Root frozen binary: 24 failures,
  exit 1; failed baseline retained.
- Candidate literal matrix: 42 C++ and 39 protocol entries pass, including
  the original full default protocol, interval-input, CTE, FETCH and HAVING
  controls. Original 384-case BIT differential also passes against 180006.
- Parser rebuilt with its own current signatures; all other 57 production
  units retain verified own-path receipts. This literal-only epoch was not
  advertised as another fresh 58-unit compilation.
- Frozen literal SHA256:
  `42aa6fe33743fa4122edb9f3ee0131830102f1f78503ee36623e2066b215f4f1`.

The repair consumes postfix fields from genuine lexer tokens and retains
actual expression source spans. It does not replace matching SQL strings.

## Column repair: contract and retained failed generations

Column and bound assignment descriptors carry an explicit interval typmod;
physical width is not used as declaration precision. Real runtime datum
casts apply the same modifier to native INSERT/UPDATE and array elements.
Catalog attributes and physical-column wire provenance carry that typmod.

Schemas with modifiers use format `0x4442000C` and an `ITM1` declaration
footer. Schemas without modifiers retain their existing format and `-1`
unbounded semantics. Independent cold-engine schema reads are tested.

Actual reference evidence includes the original 26-query reference matrix
and the strengthened 28-query matrix, both exit 0 on PostgreSQL 18.6. The
additional empty-result INSERT with typed TEXT into INTERVAL[] returns
42804 before row production; its subsequent read remains empty.

The pre-repair literal binary runs 25 column controls with 11 failures.
Its incorrect values, descriptor modifiers, error state and sequence effects
are retained in the baseline log, not converted into expected outcomes.

Initial column verification stopped at a missing test include (exit 1).
After correcting the include, complete matrix session 69747 terminated 1:

| Scope | Passed | Failed | Total |
| --- | ---: | ---: | ---: |
| C++ entries | 43 | 2 | 45 |
| Protocol entries | 38 | 2 | 40 |

The new native ALTER assertion failed because a duplicate ALTER type parser
dropped interval field precision. The original interval assignment fixture
had a stale no-op expectation for TEXT-to-array assignment. The new column
protocol entry had three failed assertions: two missing scalar typmods and
one ignored ALTER precision. The original default protocol also failed its
routine-role result assertion with empty rows; this is retained separately,
not reported as repaired or investigated in a user-filtered branch.

Final candidate corrections use shared declaration grammar in ALTER parsing
and its array-owning DDL conversion, recognize interval physical provenance
in descriptors, and assert actual 42804 in the original native fixture.
Non-array-owning declaration callers keep their existing suffix restriction.

Final matrix session 75744 terminated 1: all 46 C++ entries (including the
original ALTER type fixture) and 39/40 protocol entries pass. The final new
column protocol passes all 27 checks. The original default protocol times
out at `CREATE TEMP TABLE ctas_drop ON COMMIT DROP AS SELECT id FROM t`
(line 3488), rather than the preceding generation's routine-role assertion.
Both failed generations remain retained. No deadlines/assertions relaxed,
and no new user-filtered TEMP investigation is performed.

The original BIT differential 11874 also terminated 0: 384 controls with
zero differences against explicitly selected server 180006. An earlier
invocation omitted that reference environment and correctly rejected server
170002 before running controls; its failed log is retained separately.

Final candidate frozen SHA256:
`43b0c7cffcd4a14c07168b840435cd48c2649fb47fce6c651e7b0f63719e5812`.
Source seal:
`dc89dfbb5f098e11493b6c6289f4bde27cfb6a452a4bfc7882204a3787bd932f`.

The column public-header epoch compiled its own fresh 57 units plus Main.
Subsequent CPP-only corrections rebuilt their own affected objects. Current
58 receipts/cache/no-recompile repeat/source/frozen checks pass at gate
start and terminal state. No foreign objects are borrowed. Storage repair
`aee0b1bf` also documents the C-schema downgrade boundary in CHANGELOG.
Candidate inventory is 768 automatic native plus 2 Main drivers, 432
registered protocol/E2E entries and 58 production units. Candidate scoped
repair count is 182; master remains 180 until individual verified imports.

## Root whole-suite state

Root session 88630 continues against the unchanged production/source/test
generation recorded in `issue-interval-field-grammar.md`. Only documentation
has changed in master while its source inputs are frozen. All native/Main
entries have run; original quoted-enum, stale-temp and table-owner fixtures
failed. Protocol entries are still running. No terminal/full-pass claim.

Focused results do not close TYPE-06, PROTO-04 or any broader family. Binary
interval formats, computed-cast modifiers and other unverified requirements
remain open. The original audit checkboxes and item statuses are unchanged.

The entire original `derived_type_protocol_e2e_test.py --collect-errors`
also ran against the final interval-column frozen candidate (session 16735,
exit 1). Its only collected failure is the unchanged ordinary query
`SELECT (SELECT count(*) FROM typed_src) AS c;`, returning 0A000 with
`query requires an additional prepared plan lowering`. The planner's
`supportsPreparedSelectShape` explicitly rejects builtin aggregate roles;
repair requires actual aggregate relational lowering, not substituting the
SQL or suppressing the error. The other original collected assertions pass.
This additional diagnosis does not approve a repair or close QRY-02/QRY-05.
