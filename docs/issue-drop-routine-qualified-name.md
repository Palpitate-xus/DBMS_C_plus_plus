# Preserve qualified DROP routine names

DROP FUNCTION/PROCEDURE/ROUTINE stored individual lexer tokens in objectNames.
The DDL executor expects one intact qualified name, followed by any signature
tokens. Thus `DROP FUNCTION public.generate_series` was rejected with 0A000,
as though it named multiple targets. The original generate_series namespace
fixture retained this failure and the dependent same-name TEXT-overload cases.
An independent exact AST native regression99767 fails134 at `{public.f}`.

Use the existing qualified object-name parser for the first target, validate
the identity with the existing catalog name parser, and retain its raw quoted
components as one object name. Apply the same boundary to function, procedure
and routine. Preserve remaining signature/behavior tokens and the executor's
existing rejection of unsupported nonempty signatures or multiple targets.
Do not delete a function merely by ignoring its argument signature.

The new native regression checks intact public/quoted/embedded-dot names,
whitespace around the qualification separator, IF EXISTS/RESTRICT and malformed
targets. It also executes real function/procedure/routine deletion while
preserving a same-named function in another namespace. The independent20-case
wire fixture verifies exact schema identity, missing routines after deletion,
remaining public routine values, repeated IF EXISTS and continued connection.
All20 pass against actual verified PostgreSQL18.6/server180006.

Own initial parser CPP43479 exits0, but the first candidate is incomplete:
early native11394 fails malformed `DROP ROUTINE ()`, while qualified scalar
DROP now fails42883 because the DDL consumer passes a raw qualified spelling
to the separate name/schema storage API. Expanded58725 is deliberately stopped
143 after these known failures, not called full verification. All logs remain.

Independent function repair cced26a8 combines real routine identifier grammar
with namespace-aware storage lookup/removal and DDL records. Its four natives
and unchanged Gen78 pass. DROP20 now has18passes/two unsupported qualified
CREATE PROCEDURE dependencies, both retained. See issue-function-drop-namespace.

The next procedure candidate adds explicit schema to real procedure storage,
preserves legacy public files, decodes qualified CREATE/CALL/DROP, and carries
namespace through replacement/undo and schema RESTRICT/CASCADE. Five matching
native fixtures53400 pass, including actual rollback and an unrelated public
literal dotted name surviving schema deletion. Original drop_cascade fixture
now declares its intended procedure namespace explicitly through the API;
all restrict/delete assertions remain and public dotted-name preservation is
added. No assertions are weakened.

Actual reference32 procedure controls all pass; preceding frozen d1c8f shows23
failures. Own fresh57/44937 and Main/91300 both exit0. Initial candidate f9e439
direct wire67540 reveals nine failures: CALL mishandles whitespace around the
qualification dot, causing dependent row failures; RESTRICT's error loses
2BP01. Expanded51native/33wire55914 is deliberately stopped143 after the strong
wire failures, not because of observation timeout. Corrected own Main57191 and
DDL55555 compilation both exit0. Corrected44066 passes procedure32, DROP20,
Gen78 and the entire original procedure_replace fixture. All original/new
assertions stay intact. Normal/repeat79772 checks all58 current own receipts,
cache/source/frozen bytes and does no repeat recompilation. Final matching
five-native21277 exits0: procedure_namespace, drop_routine_qualified_name,
function_procedure, drop_cascade and ddl_transaction_skeleton all pass on the
corrected current own objects. No corrected expanded/full-suite claim. Preserve candidate
and reference expectations. No foreign object or frozen binary copying.
Original Gen43native/27wire91737 terminal1 and its two independent failures stay
preserved. Complete original default passes only in that earlier generation;
the unchanged fromless mixed pg_typeof gap still needs repair.

Artifacts:/tmp/dbms-having-grammar.MPk64yc0.
Reference:drop-qualified-reference18.log.
Original native:drop_routine_qualified_name.original.O2 (134).
Own parser compilation:drop-qualified-own-parser-build.log.
Procedure reference:procedure-namespace-reference18.log.
Original32 control failure:procedure-namespace-original-wire.log.
Initial procedure candidate:procedure-namespace-direct-wire.log,
procedure-namespace-gates.log (deliberately interrupted143).
Corrected:procedure-namespace-corrected32-wire.log,
procedure-namespace-corrected20-wire.log,
procedure-namespace-corrected78-wire.log,
procedure-namespace-corrected-original-replace-wire.log.
Corrected frozen SHA256:
dd45588df1ad410347f35e444490fc12f2a61e2dca1d81b0402285f54dca88fd.
Corrected source seal:
ae7aaf803a3a0175570edd1a032c8be4bc67cfc8a58e2f37d6364fef8bf24e21.
Broader routine overload/signature/multi-target grammar and all original
incomplete FUNC/SQL/CAT/PROTO requirements remain ordinary work. Only the15
explicit security/TDE requirements are deferred. No push or Actions activation.
