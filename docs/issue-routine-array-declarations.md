# Routine builtin-array declaration identity

The exact original CREATE FUNCTION `"Unnest"(p INT[])` failed before its query
receiver: routine declaration validation read the lexer's `INT [ ]` envelope
as an unknown scalar type. Array syntax was not transferred to the type
registry and canonical stored parameter/return identity omitted its suffix.

The shared declaration helper now requires explicit caller opt-in to recognize
array suffixes, including lexer whitespace and repeated dimensions. ALTER
COLUMN TYPE and builtin routine declarations opt in; domain and other legacy
callers keep their prior separate contract. Builtin array validation uses
Column.isArray and stored routine types retain canonical `[]`. VOID[] and
unimplemented user-type arrays are not silently turned into scalar types.

Evidence in `/tmp/dbms-routine-array-signature.LgYXa6RM`:

- Baseline 4534 exits 134 after the actual 42704 `type INT [ ] does not exist`.
- Build 57950 is normal O2, all 58 source/header/flag receipts and stamp pass.
  SHA256 `ae7e4bb07ec81eba8d0c7bdb29e93c3731b20016e0cf804c3b04eb083602b899`.
- Native 68932 passes the new routine signature control and unchanged ALTER
  array envelope/type-alias controls. Array/scalar and different-element
  replacement guards preserve the old routine when rejected.
- `expanded-reference18.log` and `expanded-candidate.log` both pass the whole
  permanent fixture: original quoted Unnest/77/OID23, actual INT[] and TEXT[]
  echo parameters, multi-parameter calls, NULL/empty arrays, canonical dimension
  aliases, return-type replacement 42P13 with rollback, unknown/VOID[] 42704.
  The reference runtime is strictly 180006. A serial repeat of the original
  standalone diagnostic also passes; its original SQL and assertions remain.
- `adjacent-candidate.log` (1783, exit 0) retains all six whole fixtures:
  PL query binding/quoted binding, scalar cardinality descriptors, typed array
  concatenation, root planning, and physical array element descriptors.

An earlier short diagnostic potentially overlapped a peer protocol batch and
is not the final serial proof. The later full group is authoritative. No
deadline, row, OID, NULL or SQLSTATE expectation was lowered.

Still open: PostgreSQL's overload-by-argument-type catalog/runtime contract,
schema-qualified/custom array types, array-valued table-function declarations,
and broader routine argument coercion. This engine's existing name-only routine
storage rejects changing array/scalar parameter identity with 42P13; PostgreSQL
can instead create another overload. The native guard proves preservation of
the existing engine contract, not full PostgreSQL overload compatibility.
