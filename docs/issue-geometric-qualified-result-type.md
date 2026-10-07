# Qualified builtin geometric result types

The stronger geometric CAST matrix exposed an independent type-description
defect: `CAST(value AS pg_catalog."path")` produced OID 25 (TEXT) instead of 602.
Pure prepared descriptors and parsed-AST inference also retained raw qualified
SQL spellings rather than the actual canonical builtin type.

The binder's CAST/`::` output descriptor, prepared default type label, parsed-AST
inference and public/storage canonical type helpers now share the raw-component
builtin resolver introduced by the preceding input fix. This is metadata only;
no function, parameter value, source or child is executed to discover a type.
Qualified/unqualified/quoted-lowercase pg_catalog names describe the same
actual builtin. A quoted mixed-case name, custom schema and quoted single name
containing a dot do not inherit the pg_catalog type by global case folding.
Raw expression bytes/spans and original CAST AST type spelling remain intact.

Evidence is retained in `/tmp/dbms-geometric-cast-input.uD4ShMCZ`:

- `meta-baseline/native.log`: terminal 134, raw qualified descriptor/inference
  types fail the 42-control pure metadata contract; sequence no-effect holds.
- `geometry-cast-input-candidate-wire.log`: terminal 1 exclusively on the seven
  qualified OID assertions, after all malformed input/effects controls pass.
- `meta-v1/geometric_cast_result_type.log`: first candidate terminal 134;
  binder/inference/type labels were fixed, but the separate public canonical
  getter still lacked this rule. The original strong assertion is unchanged.
- `final-build-v2.log`: terminal 0, 13 matching native tests pass. The 42 new
  pure metadata controls now check descriptor type, inference, default prepared
  label and public canonical type; custom-name guards and no-effects pass.
  Original input/equality/geometry, parsed metadata, primitive consumers,
  constant planner, binding/execution/cursor native tests also pass.
- `geometry-cast-reference18-v2.log`: strict PostgreSQL `180006`, complete 66
  controls pass (38 input errors and 28 valid/NULL/OID results).
- `geometry-cast-final-wire.log`: the same complete unsplit matrix terminal 0,
  original NULL PATH controls unchanged, no partial publication, no writing
  CTE sequence effect, final sentinel exactly 38 and rows unchanged.
- Final adjacent geometry typed-literal, equality, original geometry and the
  entire ordinary primitive input consumer scripts all pass (five-script
  serial group 15937 terminal 0, no remaining owned server).

Final immutable binary `final/dbms_main` SHA256:
`686a503433afa22ad3196be88192d082b9163ecdf739cbc9245e8e29e3ee04e1`.
The all-58 fresh O0 baseline and 102 headers are from this private ROOT1a plus
three ordinary input fixes, not ROOT's mutable/new-ABI objects. Every affected
object was freshly rebuilt; exact other source/header hashes are audited.
The new input header requires a fresh matching ROOT integration build. No
existing public class layout was changed by these two geometric fixes.

The full geometric CAST protocol script is now registered without a skip or
reduced assertions. This does not close every geometric CAST signature/domain,
all metadata labels or the overall type/DML family. In particular, this old
1a-based legacy FROM-less execution still emits `pg_catalog."path"` as a wire
column label although prepared AST's default label is correctly `path`.
Strict reference output is `path`; the retained
`geometric_cast_legacy_label_known_gap.py` preserves this separate consumer
check. ROOT's newer genuine FROM-less consumer must be checked independently;
its label behavior is not covered by this old private binary's green OID gate.
No push or Actions ran.
