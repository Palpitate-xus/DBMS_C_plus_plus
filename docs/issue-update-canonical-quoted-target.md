# Preserve canonical quoted DML column identities

## Reproduction and scope

On ROOT 3bc45e3b's matching optimized frozen server
`09121ee818a71dc1f34309ce4a0a827dcf257885f80964f59f7733b60700b5a6`,
`UPDATE ... SET "OnlyV"=11` incorrectly raises 42703 when only the declared
uppercase column exists. The old column lookup folds the already canonical
name a second time. Column-reference AST names are already decoded too;
RETURNING validation repeats this mistake and incorrectly falls back.

The initial V/v coexistence matrix passed even on the old server: finding v
made the mistaken validation accidentally succeed. That result is not a red
reproducer. The strengthened uppercase-only matrix really exits 1 on ROOT
(99094, ten failed assertions); its original expectations are retained. A
first lookup-only candidate still fails uppercase-only RETURNING with XX000
(84675, two failed assertions including the unchanged row). Both are retained.
Artifacts: `/tmp/dbms-update-quoted-target.nu42nILL`.

This repair removes the second folding at canonical column/ColumnRef inputs
in DML lookup, validation, predicates and RETURNING descriptor construction.
Raw assignment targets and raw aliases still undergo their first identifier
decoding. No public header, layout, storage format or manifest changes.

## Actual verification

- Normal O2 changed-DML build 37899 exits 0. The other 56 production CPPs,
  all public headers and the production manifest were compared with ROOT's
  previously freshly compiled 57-source production basis. This is a matching
  CPP-only rebuild, not a new cold 57-source build.
- Final frozen server `dbms_main.quoted-update.v2.frozen` SHA256:
  `04e035ed8546e4bf1f7c141e7c1bca7892497be6f97241adabbe57504d0945f7`.
  Historical V1 runtime hashes/logs are not reused as V2 proof.
- Native 88195 exits 0: **25 freshly linked tests pass**, including the new
  uppercase-only target, correlated OLD binding, exact RETURNING name,
  canonical integer type, NULL bit, 42703/22P02 and lock-release controls.
  Twenty-four original DML/constraint/index/WAL/page/rollback/MVCC/trigger/
  routine/typed-expression adjacent tests retain their original assertions.
- Wire 46194 exits 0: **12 protocol/E2E entry points pass**. The strengthened
  permanent uppercase-only/coexistence test 1603 also exits 0 after adding
  exact RETURNING names/OID23 assertions; it runs on the same final server.
- The exact final permanent wire test exits 0 on actual PostgreSQL 17.2,
  using a unique schema inside BEGIN, per-statement savepoints and final
  ROLLBACK. This is a 17.2 diagnostic oracle, not a PostgreSQL 18 claim.

Two newly added native-fixture assumptions failed and are retained in
`quoted-v2strong-native.log` and `quoted-v2final-native.log`: the storage
descriptor retains the legal alias `int` rather than spelling `integer`, and
a SQL NULL's display payload is not required to be an empty string. The final
test checks the canonical type and authoritative NULL bit without changing
the SQL-level value, name, error, row-count or NULL expectations. Its actual
printed descriptor is `int`; the permanent wire test separately checks OID23.

The E2E is registered after the terminal runtime group. That registry-only
addition does not change the production CPP/header/manifest or compiler ABI;
it is not falsely represented as a freshly rebuilt private cache stamp.

## Remaining boundaries

This does not close general quoted DML/query namespaces, legacy RowContext
V/v collisions, native uppercase projection, all DEFAULT/FROM/versioned
RETURNING shapes, complete assignment coercions or WITH-final DML. Those
independent issues remain open. ROOT's canonical full gate is still running;
this independently verified private commit awaits integration after its real
terminal result. No push, Actions enablement or user-deferred security/TDE
work occurred.
