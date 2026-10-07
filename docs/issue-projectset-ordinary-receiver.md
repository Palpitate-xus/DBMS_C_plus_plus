# Ordinary no-FROM ProjectSet receiver

The unchanged `unnest_e2e_test.py` returned four passes and one failure on the
old binary: `SELECT unnest(ARRAY[4,5,6])` reached a scalar prepared-plan factory
and failed with `0A000`. The same failure appears in the retained whole
ProjectSet protocol matrix after the separate pipeline and cursor-demand fixes.

Main now probes a sole direct function-call target without FROM, wholly binds
the statement, and selects the set-valued path only from the actual bound
`setReturning` metadata. It uses the same statement-owned runtime and typed
ProjectSet graph as quantified queries; routine spelling does not choose a
role. The quoted scalar routine `"Unnest"(INT)` remains scalar. Plain/ANALYZE
EXPLAIN also consumes the retained bound set-valued role. Root planning remains
explicit and children keep the default-false contract.

CLI output for this SRF receiver comes from structured cells and their real
NULL bitmap. Separators are printed between cells, not at a row's end; datum
whitespace is not trimmed. Protocol publication retains element types, NULL,
empty/text-NULL distinctions and the actual SELECT row count.

Evidence is retained under
`/tmp/dbms-explain-root-projectset.ahA83RRE/`:

- `projectset.baseline.log`, `projectset-v1.wire.log` and
  `projectset-v2.wire.log` retain the full earlier failures. V2's six remaining
  assertions belong exactly to two bare-SRF receiver queries.
- `original-unnest.baseline.log`: unchanged original CLI 4/5.
- `original-unnest.v3.log`: unchanged original CLI 5/5, original 60s command
  deadline and exact rows preserved.
- `projectset-v3.final.wire.log`: the default-registered whole 28 direct-query
  and 28 EXPLAIN-control matrix passes, including every original 22+16 control,
  the pruned-child guard, 22008/22012/21000, cumulative sequence effects,
  writing CTEs, typed NULL/empty/text data, and static errors before calls.
- `projectset.final.reference18-valid-name.log`: the same whole matrix passes
  against strict PostgreSQL 18.6 / server_version_num 180006.
- `projectset-v3-build.log` and its audits: matching independent all-58 O0
  candidate, based on a wholly fresh new-header V1 group with only PCE/main
  subsequently recompiled and every other source/header/flag checked. The
  immutable binary SHA256 is
  `080ca796b17e3ad33292f5775e6f4398ab73ab4162e1b02a1d8d514951f3db98`.
- `projectset-v3-adjacents.log`: nine distinct serial adjacent scripts all
  terminal 0 (typed EXPLAIN, read-root planning, fromless demand, quantified
  demand, unchanged stored-function clause diagnostic, WITH scalar children,
  ordinary WHERE/ORDER, and scalar-subquery sort slots). Combined matching
  native groups also pass nine distinct tests, not counting repeats twice.

This commit depends on `fc3740b0` (new explicit ProjectSet root option) and
`15e333e4` (only reached compiled quantified cursors). ROOT combination/formal
O2 verification is separate; no public layout change is added here.

## Independently open array-parameter routine DDL

The additional diagnostic retains this complete SQL without weakening its
expected success or invocation type:

```sql
CREATE FUNCTION "Unnest"(p INT[]) RETURNS INT LANGUAGE plpgsql
AS $$BEGIN RETURN 77; END$$;
SELECT "Unnest"(ARRAY[1,2]);
```

`tests/projectset_array_function_signature_known_gap.py` passes on strict
180006 (`array-signature.reference18.log`) and fails with `42704` during CREATE
on the old V2 binary (`array-signature.baseline-v2.log`). The first expanded
V3 matrix run retains its setup failure and resulting 25P02 cascade rather
than being relabeled green. This independent routine-DDL gap is not fixed by
the ordinary receiver and remains assigned for follow-up. It is an explicit
diagnostic, not an advertised supported green suite or a claim of all routine
types being complete.

Multi-SRF/physical-source ProjectSet, SRF ORDER/DISTINCT/group lowering,
correlated CTE restart, comprehensive source/cost metadata, and other planner
families remain partial.
