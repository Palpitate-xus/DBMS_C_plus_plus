# Durable INTERVAL column declaration modifiers

Scope: TYPE-06 and physical-column PROTO-04 follow-up, not either family's
completion. This change follows the separately committed postfix literal
repair `c9a25ab6`.

## Failure and repair

Column declarations previously lost interval field masks and precision.
INSERT/UPDATE/native storage, interval array elements and ALTER TYPE could
therefore store values inconsistent with the SQL declaration. Wire results
also omitted scalar physical-column typmods despite catalog metadata.

Explicit `Column::typeMod` and bound assignment metadata now carry the actual
declaration. Runtime datum casts apply its field/precision rules without
interpolating data into SQL. Catalog attributes and physical-column result
provenance preserve the modifier. ALTER consumes shared declaration grammar,
including all 13 field ranges, precision and arrays. The non-array-owning
declaration conversion keeps its existing array suffix restriction.

Schemas requiring modifiers use `0x4442000C` with an `ITM1` footer; old
unmodified schemas retain their format and unbounded modifier `-1`. The
native fixture checks an independent cold engine and persisted metadata
before and after ALTER. Older binaries cannot read modified C declarations;
the CHANGELOG documents that downgrade boundary.

## Actual verification, including failures

Artifacts: `/tmp/dbms-having-grammar.MPk64yc0`.

| Run | Actual result |
| --- | --- |
| Original PostgreSQL 18.6 reference | 26 checks, exit 0, server 180006. |
| Strengthened reference | 28 checks, exit 0; TEXT-to-INTERVAL[] assignment with empty SELECT is still 42804. |
| Pre-repair frozen baseline | 25 checks, 11 failures, exit 1; retained. |
| Initial column gate | Test compilation failed due to missing include, exit 1; retained. |
| Corrected first complete gate 69747 | 43/45 native and 38/40 protocol pass, exit 1; retained. |
| Final complete gate 75744 | All 46 native and 39/40 protocol pass, exit 1; not advertised as whole green. |
| Final new column protocol entry | All 27 checks pass, including rows, OIDs, typmods, arrays, LIKE, rename, ALTER, pure error state and unchanged sequence effects. |
| Original BIT differential 11874 | 384 controls, zero differences, exit 0 on actual 180006. |
| Original full derived matrix 16735 | Exit 1; only collected failure is ordinary scalar COUNT child 0A000, unchanged original SQL. |

The final complete default protocol test times out at its original
`CREATE TEMP TABLE ctas_drop ON COMMIT DROP AS SELECT id FROM t` query
(line 3488). The previous column generation failed a later routine-role
assertion with empty rows. Both failures remain actual failed results. No
deadline, assertion or original SQL was weakened, and no new user-filtered
TEMP/recovery/security investigation was performed.

The initial public-header epoch compiled its own 57 non-Main units and Main,
both exit 0. Later CPP-only changes rebuilt their own affected objects.
All 58 current receipts, cache signature, no-recompile repeat, source seal
and frozen binary comparison pass at gate start and terminal state. Native
tests use independent temporary data directories. No borrowed objects.

Final frozen binary: `dbms_main.interval-column-final.frozen`.
SHA256: `43b0c7cffcd4a14c07168b840435cd48c2649fb47fce6c651e7b0f63719e5812`.
Source seal: `dc89dfbb5f098e11493b6c6289f4bde27cfb6a452a4bfc7882204a3787bd932f`.

An initial BIT invocation omitted the explicit reference environment and
rejected PostgreSQL 17.2 before running controls (exit 1). Its log remains;
the explicit 180006 invocation is the reported 384-control result.

Candidate inventory: 768 automatic C++ tests plus 2 actual Main drivers,
432 protocol/E2E entries, 58 production compilation units. These inventory
counts are not a whole-suite result. Root still runs its previous sealed
generation; this private source commit is not yet Root publication proof.
Original audit statuses remain unchanged. No push or Actions activation.
