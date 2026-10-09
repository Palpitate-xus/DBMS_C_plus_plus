# SQL-value keyword grammar and typed context ownership

Actual prepareBoundQuery diagnosis69124 rejects current_user/session_user/
current_role/current_catalog/current_schema with42883. Temporal keywords also
have missing/wrong types. Explicit PostgreSQL18.6/server180006 reference verifies
ten labels/OIDs:NAME19 for five identity keywords,DATE1082,TIMESTAMPTZ1184,
TIMESTAMP1114,TIMETZ1266,TIME1083. Its pg_proc has only four corresponding real
NAME/STABLE zero-arg callees:current_user/session_user/current_database/
current_schema. Quoted current_user() is valid; bare keyword parentheses are not.

The preceding parser synthesized pg_catalog function calls for keywords during
binding, without a distinct grammar role. Missing callbacks then became42883;
temporal metadata fell back to TEXT. Native/raw parsing used ordinary columns,
and source/SQLbody/description paths could disagree about types and labels.

The candidate adds explicit keyword/precision roles, pure static types/OIDs,
execution-copy preservation, STABLE identity and reached typed-context reading.
Ordinary row columns and custom callbacks cannot replace keyword values.
The four real NAME callees have separate, arity-checked metadata; fake temporal
callees are not supplied just to make keywords work. Main admits actual keyword
projections through its retained prepared root. Original SQL remains unchanged.

Dedicated RowContext values let the existing scalar host provide its actual
user context without placing system values in ordinary column namespaces.
Current role/original login and database come from the actual runtime session
or supplied host frame; current_schema uses existing namespace metadata/markers
without creating/bootstrap-writing a catalog. No privilege/auth mutation is added.

Existing evaluator-owned UTC/whole-second clock behavior is retained, including
typed temporal precision grammar. This is not complete timezone/microsecond/
transaction-clock ownership, precision warnings, all wire typmods or general
routine return-OID/catalog implementation. Those broader requirements remain.

Artifacts:/tmp/dbms-having-grammar.MPk64yc0.
Registered actual reference29 controls exits0. The previous virtual-shadow
frozen baseline99924 exits1:29 controls/23fail, retained. New public headers
require actual own57/Main;23190/77662 both exit0 with no borrowed objects.
First native68137 exits134 because the new fixture evaluated an unregistered
expression. That authoring failure is preserved, then the fixture calls the
actual prepareExpression API without changing any expectation. Native69966
exits0 with31 controls, including pure planning, dynamic same-owner reuse,
metadata/OIDs, ordinary-column/callback isolation and real NAME callees.

Expanded initial64387 exits1:32/33native and20/21protocol pass, with its frozen
inputs. Original current_time_projection fails134 because the legacy native
adapter generates current_timestamp() from its keyword projection representation.
New wire29 has5 retained failures at NAME-return routine creation and dependent
calls; that CREATE-error branch is user-filtered and is not investigated or
removed. All other24 new controls pass. Entire original TEXT-return stored
function atomicity and SQL query-body entries already pass, as do the original
pg_stat_activity/pg_settings entries. This is not a whole-gate pass.

All58 current own receipts/cache/repeat/source/frozen terminal fences pass;
the entire default protocol also passes in this failed generation. Native
grammar adapters now retain the legacy keyword projection role instead of
inventing a zero-arg routine. Four TEXT-body controls are added to the existing
whole fixture without deleting or relaxing any of the original five NAME
failures. The whole 33-control reference fixture subsequently exits0 on actual
PostgreSQL18.6. Corrected whole gate91913 exits1:33/33 native pass and19/21 wire
pass. The new fixture has28/33 passing controls and the same five retained NAME
creator/dependent failures. Both original TEXT-body fixtures and four additional
TEXT-body controls pass; they do not replace the five NAME assertions.

The other wire failure is the unchanged default protocol fixture: socket timeout
at CREATE TABLE sub_text. An unchanged repeat66108 against the exact same frozen
binary also exits1, at CREATE TABLE sub_outer. Both logs are retained; the earlier
default-protocol pass does not make this corrected generation green. No timeout
or assertion was widened, and the user-filtered CREATE-error branch was not
investigated. All58 actual own current receipts, cache signature, no-recompile
repeat, source and frozen-binary terminal fences pass in corrected gate91913.

The scoped keyword grammar/type/context repair is committed separately with
this partial verification evidence. NAME creators, complete default protocol,
public parser invalid-input contract and broader temporal requirements remain
open; neither the complete SQL-value family nor the whole gate is marked done.

Initial frozen:dbms_main.sql-value-keyword-initial.frozen.
SHA256:03b710f77d8f451c1961c806e9dd440a0ef9bd1704e0ac8f3e9ae0f3728a2376.
Source seal:41749ce57d487ee081c996a1ebbde449cb8e5151f32192eb0ce6a3e8b701bdc1.
Fix the actual native grammar adapter after terminal; preserve complete NAME
controls and add ordinary TEXT-body positive controls, not substitutes.

Master full18383 remains live on source795479c6/frozen02c69e42 with773native/Main/
435wire. Master186source and private187 committed source remain distinct;
SQL-value repair is independently committed in the private worktree, not yet
published to master while its full regression is live. Original273 scope/statuses and all retained
failures remain unchanged. No push, Actions activation or filtered restart.

Corrected frozen:dbms_main.sql-value-keyword-corrected.frozen.
SHA256:0e7c337827d5c0c2597e86c3083b29d1d39a1a5674f00ca9de0ff11209d8b597.
Source seal:45d15e52af6759a23e78497439f92b9355b3ee6e95059f99666e66d386e86e89.
Logs:sql-value-keyword-corrected-gates.log,
sql-value-keyword-corrected-default-repeat.log,
sql-value-keyword-text-added-reference18.log.
