# TypeName parser checkpoint: independent partial repair, Goal remains open

Published Root stays `ea67ad29` /123 source-test repairs. The eight enum
candidates remain held separately. Independent local commit `e4f16819` on
`fix/root-typename-0mADrqkN` repairs the common declared-type envelope for CAST
and ::. It is not imported into master and is not TYPE-11/family completion.

## Actual evidence

Artifacts: `/tmp/dbms-root-typename.0mADrqkN`.

| Complete exact input | Actual outcome |
| --- | --- |
| Root123 original new68 native declaration controls | 134;all68 reached,40 failures |
| Independent original45 paired TypeName queries on actual Root | 1;45 reached,41 differences against owned180006 |
| First parser candidate | Native abort on malformed cast; patch, frozen binary and log retained, not PASS |
| Corrected same unchanged68 native assertions | 0;all68 |
| Current complete35 native | Session7717 actual0 |
| Original complete28 protocol | Session60174 actual0 |
| Corrected parser same full45 paired queries | 1;45 reached,38 differences |
| Final permanent ours45 | 1;45 reached,38 differences |
| Final permanent owned PG18.6 reference45 | 0;45 reached,zero differences |

The three newly corrected real executions are modifier-bearing TIME and
TIMESTAMP followed by WITHOUT TIME ZONE, and BIT VARYING with both a modifier
and an array suffix. The parser also retains quoted/qualified type names,
negative numeric scales, aliases, actual parameter source coordinates and
strict rejection of incomplete type/modifier/array declarations. It does not
accept an uncaught parser exception as the desired API result: syntax-only
declaration failures use the existing invalid-expression boundary.

Normal epoch2 session57195 is actual0: one genuinely fresh changed Parser CPP
plus57 own unchanged source/header/compiler/flag/manifest/receipt/ABI and
byte-proven actual Root123 objects. All58 current-path receipts/cache,
no-recompile repeat, input seal and frozen bytes are checked. It is not fresh58
or previous-header reuse. SHA-256:
`1312a0965461e1e50b40b04ca548435949a257b82528ef7dbdd79ccdd420abde`.
The later additive permanent corpus/registration relinks without CPP changes
and retains identical production bytes; the earlier35/28 belong to their
original sealed epoch, not an invented broader pass.

Permanent45 has default ours mode and an independent `--reference18` mode.
The SQL list is checked exactly, and expected builtin OIDs/values/states/names/
tags are copied from actual owned PG18.6 records, not ours. JSON's outer list
versus the wire decoder's tuple is normalized without changing nested values;
the initial erroneous container-comparison run and red reference log remain
preserved. All45 original semantic assertions remain. Private inventory is
729 auto-native plus two actual Main frontends/403 registered/58TU, not the
published Root728+2/402/58 inventory.

## Continuing full plan

1. Generic typed-constant TypeName grammar, including VARBIT, multiword,
   quoted/qualified declarations, modifiers and real string input. A keyword
   whitelist extension alone cannot close the demonstrated requirement.
2. Actual catalog namespace/type/OID resolution for both constants and casts.
   Unknown types/schemas currently sometimes become successful TEXT
   passthrough and must produce real errors without guessing from row values.
3. Correct runtime conversion and pure descriptors. Preserve actual reference
   bare BIT constant length versus explicit CAST default-length behavior.
4. Rerun the full45 and original immutable BIT384 (its16 typed VARBIT reds
   remain required), complete native/protocol/phase/demand controls and all
   original unmet families. The broader registered29 counterpart is red,
   not approved by the narrower original28 pass.

## Declared-constant candidate (held, not integrated)

Independent local commit `63e226bb` on `fix/root-type-constants-ZF9xcCr9`
adds generic type constants, copied-catalog type lookup, builtin input
conversion and canonical builtin descriptors. It removes the textual
keyword/string-to-`::` rewrite that changed bare BIT length semantics and
could corrupt qualified names or escaped quotes. Published master source
remains `ea67ad29`; the README-only update is `05470414`.

Artifacts: `/tmp/dbms-root-type-constants.ZF9xcCr9`.

| Complete exact input | Actual outcome |
| --- | --- |
| New native69, corrected real catalog initialization, unchanged expectations, on parser-only baseline | 1;69 reached,68 failures |
| Current native69 on candidate epoch2 | 0;69 reached,zero failures |
| Current full45 protocol | 0;45 reached,zero differences |
| Owned PG18.6 full45 reference | 0;45 reached,zero differences |
| Original immutable paired BIT384 | 0;384 reached,zero differences |
| Original28 plus three additive protocol fixtures | 0;31 complete invocations |
| Original35 plus three additive native fixtures | 1;38 complete invocations,10 failed fixtures |

Epoch1 compiled all58 production units genuinely afresh. Epoch2 rebuilt only
NetworkServer and expr_helper against those same unchanged headers; all58
current-path source/header/compiler/flag/manifest receipts, the normal cache,
no-recompile repeat and frozen bytes are verified. Epoch2 binary SHA-256:
`1873b89606ad83918a47a50127abc213e3ec9a80d68c5510cb695f46b10e70ed`.
Full input seal:
`54584889add4d2f92c6736537badd68c1055656d4ed24df345a4ed89845883a3`.
The first epoch's six genuine OID differences, initial native setup failures
and all subsequent red native logs remain preserved; none is labelled PASS.

The candidate is **not approved for integration**. The complete native gate
found cold, uninitialized catalog lookup regressions and a separate grammar
regression: `CASE 'zeta'::rank_type WHEN ...` is intercepted as a type
constant rather than a CASE expression. Neither the successful45/384 nor the
successful31 protocol fixtures closes those failures. A separate cold lookup
repair and authentic empty-catalog/no-bootstrap native test are in progress at
`/tmp/dbms-root-type-cold.b3XG2ENF/repo`; no result is claimed for that WIP.

Next: fix cold builtin lookup without bootstrapping during preparation;
restore expression grammar precedence for CASE; rerun the same complete
native and protocol gates, including full45/384. General custom type input,
domains, search-path shadowing and all other original unmet families remain
open pending their own evidence.

Every original273 item/hash/status stays unchanged:22 complete,166 partial,
70 unverified,15 user-deferred. No original full-suite/SAN, general TypeName,
enum or entire Goal completion is claimed. No push, Actions activation,
skipped security/TDE or filtered CREATE restart occurs here.
