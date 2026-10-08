# Shared declared-type grammar for CAST and :: (partial TypeName work)

This independent parser repair is based on published Root `8501bf38` /
production `ea67ad29`. It does not import the eight held enum candidates.

## Real failures and finite repair

CAST's private scanner stopped after its modifier list. Legal timezone
clauses and array suffixes consequently fell through as malformed function
arguments. The separate :: scanner greedily consumed output aliases and
accepted malformed modifier/array/type envelopes. Both now consume one
genuine declaration using the same existing declared-type parser as DDL.
Cast nodes retain modifiers, timezone identity and the array envelope;
postfix casts retain their established binary role and original parameter
byte provenance. Syntax-only declaration failures return an invalid
expression at the existing parser boundary, not an uncaught exception.

The actual Root baseline evaluated all68 permanent native controls and
failed40 (terminal134). The same unchanged68 assertions now pass. Current
complete35 native and original28 protocol gates also pass. The first candidate
improved valid declarations but aborted on a malformed cast; its patch,
normal frozen binary and failed log remain preserved. No assertion or SQL is
changed to hide that failure.

## Stronger complete TypeName evidence remains red

The independent45 paired queries compare actual Root with owned
PostgreSQL18.6. Baseline45/41 fails; parser candidate45/38 fails. The three
newly repaired executions are TIME precision followed by WITHOUT TIME ZONE,
TIMESTAMP precision followed by WITHOUT TIME ZONE, and a modifier-bearing
BIT VARYING array CAST. The other38 are still failures, not a completed
TypeName/type/SQL family.

All45 original SQL/value/state/name/tag/OID controls are retained permanently
in `tests/typename_constants_protocol_e2e_test.py`. Its expected records are
copied from the actual owned strict reference, including builtin OIDs, not
from ours. Default mode runs ours without requiring an external reference;
`--reference18` independently verifies the same full corpus. An exact source
SQL list check prevents losing a case. JSON's outer list versus the wire
decoder's tuple is normalized without changing any nested expected value.
The initial erroneous container comparison and failing reference run remain
preserved. Final permanent ours45/38 is actual1; reference45/0 is actual0.

The original immutable BIT384 matrix and its16 typed VARBIT failures remain
required; this parser shape repair does not stand in for their completion.

## Production input proof

Artifacts: `/tmp/dbms-root-typename.0mADrqkN`.
The normal build compiles the sole changed Parser CPP. All57 other objects
are individually source/header/compiler/flag/manifest/receipt/ABI and byte
proven against actual published Root123. Current all58 receipts/cache,
no-recompile repeat, input seal and frozen binary are checked. The correction
again compiles only the changed Parser with all57 own unchanged objects.
This is not a fresh58 build or previous-header object reuse.

Parser epoch2 binary SHA-256:
`1312a0965461e1e50b40b04ca548435949a257b82528ef7dbdd79ccdd420abde`.
Native35 and whole28 use the epoch2 sealed source/test combination. The
subsequent permanent45 corpus/registration is additive; its broader whole29
counterpart is still red and not described as an approved full integration.

## Next owners, not narrowed completion

1. Generic typed-constant declaration grammar: VARBIT, multiword, quoted and
   qualified names, modifiers and actual string inputs, not another whitelist.
2. Real catalog/type namespace and OID lookup for constants and casts;
   unknown types/schemas must not become successful TEXT passthrough.
3. Runtime input conversion and static descriptors, preserving the reference's
   bare BIT constant length versus explicit CAST default-length distinction.
4. Pure Parse/Describe, actual demand/effects, all45 and original384, and every
   wider original unmet owner. No full-family or273-item completion claim.

The rules are checked against actual owned180006 execution; relevant primary
references are [PostgreSQL18 typed constants](https://www.postgresql.org/docs/18/sql-syntax-lexical.html#SQL-SYNTAX-CONSTANTS-GENERIC)
and [bit strings](https://www.postgresql.org/docs/18/datatype-bit.html).
No push, Action activation, skipped security/TDE or filtered CREATE branch
restart occurs in this independent ordinary declaration repair.
