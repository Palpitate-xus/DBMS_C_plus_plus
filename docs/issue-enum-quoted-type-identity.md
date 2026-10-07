# Enum CREATE TYPE identifier identity

## Actual defect and bounded change

`CREATE TYPE "MixedRank" AS ENUM ('zeta','','alpha')` previously retained the
raw quoted token as both the catalog type name and the enum sidecar name.
Subsequent SQL type binding decoded that identifier, so a direct enum comparison
returned lexical `f` instead of declaration-order `t`, and the original quoted
CASE control failed with 42883. A real native catalog assertion also failed:
the actual `public` namespace contained no canonical `MixedRank` enum type.
This is a name-identity mismatch, not a comparison of unrelated PostgreSQL and
engine custom OID numbers.

CREATE TYPE now uses the parser's existing SQL identifier decoder for its type
and optional schema tokens. It preserves quoted case and folds unquoted case.
The corresponding DROP TYPE consumer folds unquoted qualified-name components
while preserving quoted components. The latter is necessary for the native DDL
API too: after correctly creating `UPPER_RANK` as `upper_rank`, its original DROP
consumer still searched for the raw uppercase name. A strong new native DROP
assertion fails against the CREATE-only candidate and passes after this repair.

Production changes are only those two CREATE parser lines and the DROP TYPE
`parseQualifiedName(..., true)` argument. COLLATION code is unchanged. No public
header, catalog format, sidecar format, OID allocation rule, SQL value or index
assertion is changed in this follow-up.

## Permanent controls

`tests/enum_quoted_type_identity_test.cpp` checks actual canonical catalog and
sidecar names, absence of raw quote-bearing names, distinct type OIDs in `public`
and `"ScopeRank"`, opposite declaration ranks, binary/prefix casts, CASE and
constant planning, unquoted uppercase folding, actual cold catalog/sidecar reads,
exact quoted DROP identity, and native uppercase-unquoted DROP followed by actual
catalog and sidecar absence.

The registered `tests/enum_comparison_binding_protocol_e2e_test.py` retains every
original SQL statement, row/type/SQLSTATE assertion and timeout. Seven additive
controls verify uppercase-unquoted CREATE/cast/DROP, coexistence of quoted
`"MixedRank"` and unquoted `mixedrank` with opposite ranks, and survival of the
quoted type after dropping the case-distinct unquoted type. The complete wrapper
still includes all seven operators and labels, NULLs, projection/WHERE/ORDER
consistency, CASE/CTE/scalar-child identity, quoted schema/type controls, indexes,
22P02/42883/42846 errors and an actual cold process restart. The strict PostgreSQL
18.6 version gate is unchanged; reference creation is restricted to a unique
owned schema in a rolled-back transaction, not shared `public`.

## Actual evidence and production provenance

Private base is the frozen projection prefix
`6593bf29a7ee209ce2a7b24f3db14346a36ba88c`, not ROOT's newer BIT/hash/startup
composition. The original prefix tree and every original failing/passing log
remain frozen. The baseline production build proves all 58 normal donor source,
header, compiler, flag, manifest, receipt and object inputs, with SHA-256
`1e6abb2c55562d85ad25a7ff4748f2c33de4c5fe0a75e810ac1b8efba52c8aac`.

- Original quoted native identity control on default disk: terminal 134 at the
  exact canonical catalog-name assertion.
- CREATE-only Source1: complete original quoted wrapper on default disk and
  strict real PostgreSQL 18.6 (`180006`) both terminal 0. Complete 30-driver
  native group with `TMPDIR=/dev/shm` and the three original Btree/HASH/non-enum
  CASE wire drivers on default disk are terminal 0. These are intermediate
  evidence, not final evidence for the later DROP change.
- Source1 with the added exact uppercase-unquoted native DROP control: terminal
  134 (`DROP TYPE UPPER_RANK` cannot find the canonical lowercase type).
- An intermediate Source2 build accidentally changed the unrelated shared DROP
  COLLATION parse context. Its logs and binary are preserved but discarded; no
  candidate regression or final readiness claim uses it. The final production
  diff explicitly verifies that only DROP TYPE changed.
- Final Source3 normal build/repeat and all-current-58 receipt/stamp/frozen/input
  audit: terminal 0. The actual parser and correct DDL consumers were freshly
  compiled with normal production flags; the other 56 objects match the proven
  donor's production sources, public headers, compiler/flags and object bytes.
  This follow-up is not falsely described as another fresh-all-58 invocation.
  Frozen SHA-256:
  `47e16efc4eb66c10c7669704e2981ffbaa863787a0198613a1234b4618770aed`.
- Final strong quoted native on default disk: passed. Final complete extended
  quoted wrapper plus original Btree/HASH/non-enum CASE drivers on default disk:
  terminal 0. The full unchanged strict 180006 extended quoted wrapper and
  original Btree/HASH reference matrices are terminal 0.
- Final complete 30-driver native group with `TMPDIR=/dev/shm`: terminal 0,
  including both new complete drivers and all 28 original neighbours. Every
  requested driver actually ran; no assertion, normal flag or original control
  was shortened to obtain that result.

External frozen binaries, normal build/repeat/audit logs, strong native failures,
complete drivers and strict reference evidence are under
`/tmp/dbms-enum-type-identity.nMShYjUm/`. The private inventory is 688 automatic
native drivers plus the actual frontend driver, 350 registered protocol drivers
and 58 production translation units. Passing the focused 30-driver group is not
a claim that that entire inventory, ROOT composition or TYPE-08 passed. ROOT's
future integration of the projection prefix must preserve its independent BIT
and hash changes and freshly compile all 58 against the three changed public
headers before making ROOT-specific claims.

## Explicitly remaining OPEN

This item fixes ordinary enum creation identity, the related genuine casts/CASE
and dependency-free DROP controls above, not every quoted-type lifecycle:

- The physical quoted enum column consumer still lowercases its type spelling.
- The quoted ALTER TYPE compatibility consumer still mislocates the action after
  decoding the identifier. Added-label transaction safety/rollback and empty or
  escaped ALTER label grammar remain separate open issues.
- Dot-containing quoted identifiers still conflict with the existing flat
  `schema.name` enum sidecar encoding. Generic namespace/owner work is not part
  of this item, and the old DROP physical-dependency scan remains case-insensitive.
- Existing malformed raw-quoted or raw-uppercase persisted metadata is not
  silently migrated or falsely claimed repaired.
- Custom enum protocol OID publication and general aggregate expression
  consumers remain open. The complete owned strict reference aggregate probe
  gives `SUM(CASE WHEN r < 'alpha' THEN 1 ELSE 0 END)=2` with INT8, while the
  candidate still returns `0` with TEXT; `bool_and(r > 'alpha')` returns SQL NULL
  instead of false. `COUNT(*) FILTER (WHERE r < 'alpha')` correctly returns 2.
  The final Source3 full aggregate diagnostic probe and owned 180006 reference
  probe confirm these same remaining differences after the completed combination
  gates. These failures are retained separately and never relabeled whole
  TYPE-08 green; diagnostic process exit 0 is not a matching-results claim.

Default-disk evidence and the explicitly scoped tmpfs native runs are separate;
no tmpfs green result overwrites a default-disk failure. No push or Actions change
is performed.
