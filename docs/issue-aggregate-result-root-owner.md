# Aggregate result metadata belongs to the actual outer expression

This is a separate ordinary aggregate-result metadata issue after the frozen
enum argument-binding prefix `6c75dd0e`. It does not change argument evaluation,
enum operator binding, input RID demand, reducers, or locale configuration.

## Real failure and owner

The complete enum argument matrix on the exact `6c75dd0e` binary still returned
the correct values but the wrong second result OID for the unchanged statement:

```sql
SELECT every(r<'alpha'),sum(CASE WHEN r IS DISTINCT FROM '' THEN 1 ELSE 0 END)
FROM ranks WHERE id<=2;
```

The actual row was `['t','1']`; result OIDs were `[16,16]`, not `[16,20]`.
The exact prefix's whole exit 1 and its original SQL/assertions are preserved.
Its earlier 53-failure pre-binding baseline also remains unchanged.

This metadata root is not enum-specific. `ExprHelper::inferResultType` parses
the expression, but formerly admitted only unary/array/selected binary roots
before a global substring predicate shortcut. That shortcut saw `IS DISTINCT
FROM`, `BETWEEN`, `LIKE`, or `ILIKE` inside a SUM/COUNT/MIN/MAX argument and
declared the whole outer expression boolean. Predicate words inside a string
value could trigger the same wrong OID. Parsed aggregate overload inference
already knew the correct result; the string consumer chose a nested role first.

## Narrow production correction

The only production file changed by this issue is `expr_helper.cpp`:

- Admit an actual outer FunctionCall AST for the existing eight reducers before
  nested textual shortcuts, using the existing AST argument/value-arm typing.
- Decode the actual reducer identifier with the existing qualified-name parser;
  only unqualified or actual `pg_catalog` names acquire this builtin role.
  Quoted lowercase builtin names retain their actual identity, not a raw token.
- Preserve existing bound `resolvedResultType` and actual routine metadata
  precedence. A qualified stored routine named `sum` returning TEXT retains
  OID 25, its actual scalar row count, and its declaration during Describe.
- Preserve outer comparison/arithmetic/CASE ownership: SUM(INT) yields BIGINT,
  SUM(BIGINT) yields NUMERIC, but `SUM(...)>1` remains boolean. No constant
  descriptor is supplied to turn arbitrary SUM inputs into BIGINT.

No public headers, Main dispatcher, TableManage, parser, function execution,
or collation defaults are changed by this metadata issue. The accepted Root
Boolean BETWEEN grammar inference prerequisite is imported in the separate
private commit `ddeaa25f`, with the exact `3ffbb516` production hunk and its two
original permanent native/protocol tests. The old private baseline's four real
BETWEEN CASE 42804 failures are not reclassified as this metadata bug or erased.

PostgreSQL 18's [aggregate overload and empty-input contracts](https://www.postgresql.org/docs/18/functions-aggregate.html)
agree with the runtime controls: integer SUM is BIGINT, bigint SUM is NUMERIC,
MIN/MAX retain their input type, COUNT is BIGINT, and empty SUM is SQL NULL.

## Strong permanent controls

`aggregate_result_root_owner_test.cpp` retains all 34 actual parsed expression
controls. They include DISTINCT/NOT DISTINCT/BETWEEN/LIKE/ILIKE nested CASE,
enum-label predicate versus integer value arms, BIGINT/REAL typed column hints,
typed NULL/casts, AVG/MIN/MAX/COUNT/boolean reducers, actual qualified and quoted
builtin identities, predicate words inside literal values, FILTER, outer
arithmetic/comparison, standalone predicates, and the earlier SUM money/real
and AVG interval overloads. The original nullable-parameter metadata test runs
unchanged alongside it.

`aggregate_result_root_owner_protocol_e2e_test.py` checks actual rows, tags,
headers and OIDs, empty tables, FALSE input, LIMIT 0, typed NULL and BIGINT
overloads, scalar predicate results, and Parse/Describe without DataRow or
function effects. Actual writer execution demands only the selected input
occurrence. The user routine named `sum` is a real additional ownership guard.
The additive collector still ends nonzero when any strong assertion fails.

The complete existing enum argument protocol file remains byte-for-byte
unchanged from `6c75dd0e`; no original SQL, values, NULLs, strong assertion,
reference-version requirement, timeout, or error control was weakened.

## Actual build and evidence

Private tree: `/tmp/dbms-aggregate-result-owner.UU589MFz/repo`.
The exact normal `6c75dd0e` donor is the immutable tree
`/tmp/dbms-enum-rank-reductions.zCaoMHUM/repo`; its original candidate binary,
all 58 production sources, all headers, actual compiler/options/manifest,
genuine receipts/stamp, and copied object bytes were checked before bootstrap.

Candidate Source1 SHA-256:
`8e17dc6fff053ed683ee2e7053629c4b2046461005defd2b4e34b6a26766dfe4`.
Only `src/expression/expr_helper.cpp` was freshly compiled in normal O2; the
other 57 source/object bytes are identical to that verified donor. All 58
current path/source/header/compiler/flag receipt signatures and the build
stamp were then independently rechecked. Normal repeat compiled no TU and
matched the frozen binary. This is a genuine private legacy-basis proof, not
the current Root public-window/constructor/integers ABI integration proof.
Private inventory is 692 auto-native files plus the actual frontend,
356 registered protocol drivers and 58 production translation units.

| Complete scope | Actual terminal |
| --- | --- |
| Exact prefix original enum argument matrix | `45259=1`, exactly one metadata OID failure |
| New ordinary metadata baseline, same final fixture and frozen prefix binary | `92412=1`, 25 strong failures: 21 descriptor failures and four accepted-prerequisite BETWEEN 42804 failures |
| New native baseline after correcting only a nonexistent include filename | `50361=1`, actual new root test abort 134; original nullable-parameter metadata test passes |
| Normal Source1 build and repeat | `47582=0`; sole actual fresh ExprHelper, repeat no compile |
| Formal all58 input/object/receipt/stamp/freeze audit | `89262=0` |
| New native 34-root and original metadata pair | `14671=0` |
| Seven complete native neighbours, default disk | `89177=0` |
| Fifty-two complete native neighbours, explicit TMPDIR=/dev/shm | `39514=0`; distinct diagnostic scope, not a substitute for default disk |
| Original complete enum argument matrix | `37316=0`; includes the previously red EVERY/SUM statement unchanged |
| Original 25 whole neighbours plus BETWEEN/new metadata/full enum argument | `19112=0`, all 28 default-disk drivers, no SQL/assert/deadline changes |
| Owned strict matched-default PostgreSQL 18.6 reference | All complete new metadata/new enum argument/original enum comparison/original generic argument/original BETWEEN matrices actually exit 0 with version 180006 |
| Original complete expanded CTE/derived/clause OPEN neighbours | `69213=1`, all three whole drivers genuinely nonzero: the same nine older expanded owner failures, one scalar COUNT 0A000, and one scalar UNKNOWN XX000 |

Strict runs use the existing owned `127.0.0.1:15487` database
`dbms_oracle_en_us_20261006`, unique owned transaction schemas and ROLLBACK.
No shared public writes, database configuration, creation/drop, password
printing, push, GitHub Actions enablement or deferred security/TDE work occurs.

Logs and exact input helpers are preserved beside the private tree:
`bootstrap.log`, `baseline-complete-enum-rank.log`,
`baseline-complete-result-root.log`, `baseline-complete-result-root-final.log`,
`baseline-native-root.log` (authored include failure, not a production result),
`baseline-native-root-v2.log`, `source1-normal-build.log`,
`source1-normal-repeat.log`, `source1-formal-input-audit.log`,
`source1-complete-enum-rank.log`, `source1-complete-result-root.log`,
`source1-complete7-default-disk-native.log`,
`source1-complete52-explicit-tmpfs-native.log`,
`source1-complete28-default-disk-whole.log`,
`source1-complete-original-open-cte-neighbours.log`, and the five `strict18-complete-*`
final/reference logs. Earlier raw-C-locale fixture failures belong to the
frozen generic-argument issue and remain preserved; no default-locale change
is made here.

## Scope remains open outside these two aggregate issues

The previously confirmed expanded CTE owner errors, scalar-child COUNT lowering
and scalar-child UNKNOWN remain independent OPEN roots, not renamed PASS.
Their full original additive collectors have actually completed as a separate
explicitly OPEN neighbour gate; their true nonzero results are not included in the scoped 28
passing-driver count. No claim of full project or TYPE-08 completion is made.
Custom wire enum result OIDs, MIN/MAX's separate fresh enum comparison receiver,
ALTER/new-label transaction semantics and the other still-open type checklist
items require their own independent fixes and complete original controls.

Future Root integration must retain its current Boolean BETWEEN inference,
Window public ABI, enum binding/CTE source owner, BIT constructor, hash NULL
consumer and integer transformations. The earlier enum argument alias adds a
public TableManage header field, so the actual Root combination requires a
truly fresh all58 build; private legacy files or donor objects must not replace
those newer Root production inputs.
