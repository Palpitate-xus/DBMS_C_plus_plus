# BETWEEN result descriptors belong to the actual outer AST

Root 104's helper already knows that the parser-owned `BETWEEN`/`NOT BETWEEN`
function-call node returns Boolean. Its structural early-return admission did
not include that role, however. A later textual scan interpreted the bound's
postfix CAST as the type of the entire projection: `NULL BETWEEN NULL AND
NULL::smallint` emitted a SMALLINT descriptor although its value was Boolean
NULL. This also affected INTEGER/BIGINT, empty projections, and Describe.

There is a second entry of the same metadata-root problem: the scan could
report Boolean for `CAST((1 BETWEEN 0 AND 2) AS text)`, instead of respecting
the genuine outer CAST. This does not authorize searching arbitrary nested
text for a type: structural inference must identify the actual root role.

The finite correction accepts a real, unqualified, three-argument parser
grammar node, either as the root or the direct operand of a genuine outer
`CastExpr`, in the existing structural inference path. The role is identified
by its parser-owned uppercase `BETWEEN`/`NOT BETWEEN`, not by folding arbitrary
routine names. Its actual outer AST still determines the result. No public
layout, aggregate inference, enum/domain binding, storage owner, evaluation
order, or callback execution is changed.

Permanent controls:

- `between_result_type_test.cpp`: 61 actual parser/helper metadata controls.
  Six bound type spellings, both grammar operations and both NULL/UNKNOWN
  prefixes, standalone and outer postfix CASTs, five true outer CAST targets,
  and actual quoted/schema-qualified stored routines named `between` and
  `not between` are checked. Built-in character aliases are compared through
  the actual TypeRegistry identity, not an arbitrary raw alias spelling.
- `between_result_type_protocol_e2e_test.py`: all 32 complete wire queries,
  including real stored SQL routines in a unique owned schema. It asserts the
  exact data value, NULL, name, tag, OID, width, typmod and format.
- The separate Boolean-character fixture retains 105 strong wire controls,
  including 30 true outer CASTs of range results, exact character typmods,
  and genuine BOOLEAN Parse/Describe/Bind/portal Describe/Execute inputs.
- The full integer input fixture retains all 5208 rows/error/OID controls;
  its input-only v1 build still failed 774 old descriptors. They are not
  omitted, recast as a passed subset, or described as an input conversion fix.

Evidence is preserved in `/tmp/dbms-integer-between-input.fHTGGaWj`:
`between-result-new32-baseline-v1.log` actually returns 1, with 12 integer-bound
descriptor differences and one independent old Boolean-to-TEXT value error.
`between-result-new32-candidate-v2.log` retains that independent value error.
After the separate Boolean-character correction,
`between-result-new32-candidate-v3.log` actually returns 0, and the final
20-file strict PostgreSQL 18.6/180006 reference batch returns 0. Intermediate
native fixture drafts incorrectly demanded `character varying`/`character`
where the established metadata API legitimately returns `varchar`/`char`;
their actual failed logs remain, and final tests use real normalized type
identity while preserving all value/null/cursor assertions.

The final combined binary and complete native/wire terminal gates are recorded
in `issue-integer-between-input.md`. This is a separate metadata-root commit,
with the input-preparation and Boolean text-output fixes kept independent.
Neither BIT(0)/VARBIT(0), VARBIT alias/catalog parsing, old physical prefix
comparisons, nor the six old writer-demand differences are claimed fixed.
