# JOIN USING merged-column declaration type

The binder previously copied only the left input's declared type into the
merged USING occurrence. An INTEGER/BIGINT FULL JOIN could therefore expose
an unmatched BIGINT value as INTEGER. The new private FROM mutation consumer
reproduced `22003` for a valid `2147483648` merged value and arithmetic target;
strict PostgreSQL 18.6 returns all three values with OID 20.

The merged descriptor now uses both input declarations and the shared pure
`selectCommonType` rule (dependency `8e2c2076`). This commit changes static
metadata only; it neither invokes routines nor reads/evaluates source rows.
The separate FROM/USING consumer uses genuine implicit positional casts for
the comparison and output, rather than renaming the cell's type.

Evidence under `/tmp/dbms-with-source-runtime.zl5yD3MZ`:

- `boundary-reference-v1.log`: strict reference `180006`, terminal 0.
- Frozen candidate v2 `c68828dd…` reproduces the actual wide-value `22003`
  (`with_multisource_dml_boundary_protocol_e2e_test.v2.log`); this failure is
  retained alongside the distinct WHERE false/NULL ON-side-effect failures.
- A freshly compiled preceding binder (`b7dd959a`) linked with the matching
  new-header group fails the new native metadata assertion (session 16694,
  terminal 134; `join-type-native-baseline2.log`). The first minimal link
  attempt missed the evaluator dependency and never ran the test; its
  terminal 127 log is not a correctness baseline.
- Fresh candidate all 58 objects/stubs, source/header audits: session 40855,
  terminal 0. The new native test covers both operand orders, INNER/LEFT/
  RIGHT/FULL, NATURAL numeric widening and precise `42804` for INTEGER/TEXT
  (matching native group 95380 terminal 0).
- Separate runtime v3 consumes these declarations with actual casts; the
  unchanged 47-control matrix and five boundary controls both pass (35473
  terminal 0, frozen binary `0e44e362…`). This is consumer evidence, not a
  claim that this metadata commit alone enables FROM/USING execution.

Builtin common-type rules remain the covered contract. Domain-base/user
cast catalogs and the complete SQL operator/type family are still partial.
