# Shared function/provider search_path selection

The independent matching baseline creates a public scalar `pg_listening_channels`
and accepts `SET search_path=public,pg_catalog`, yet the bare SELECT still returns
the empty builtin provider result. Strict PostgreSQL 18.6 returns the public
scalar. That diagnostic isolates provider selection from the separate SET LOCAL
and qualified-declaration failures; it is not a weakened passing test mode.

The scalar binder and registered query-host providers now consume one pure
metadata resolver. Effective routine namespaces preserve explicit pg_catalog
placement; catalog is prepended only when absent. Missing namespaces and implicit
temporary relation namespaces do not become routine candidates. Stored callable
identity includes the actual namespace, database and engine, and execution invokes
that same record. No provider row, function invocation or evaluated value is used
to choose identity/type.

The actual registered fixed zero-argument provider competes in namespace order.
For the registered polymorphic array provider, visible exact typed scalar
candidates win; unknown input retains array ambiguity except for an actual string
category scalar candidate. The original TEXT-shadow native assertion exposed the
first candidate's overly broad unknown ambiguity rule; its failure is retained,
and the final matrix adds NULL/string/TEXT and typed-array controls rather than
deleting that original assertion.

The ordinary entry uses resolved stored identity to retain the complete bound AST
before any target runs, preventing legacy backend-name dispatch from taking a
scalar shadow. Explicit pg_catalog still uses the real NotificationManager
backend/database subscription reader. Its staged LISTEN/UNLISTEN and rollback
behavior, zipped typed NULLs and terminal ProjectSet semantics are unchanged.

Actual callback slots retain their bound AST occurrence identity before SQL
namespace lookup. A real public routine with an identical internal spelling does
not change that site's type/volatility/identity on rebinding. Another SQL AST
occurrence still selects that public routine; it does not acquire the callback or
a fictitious pg_catalog function through a name-prefix exemption. The native
identity control's first real failure and its corrected result remain preserved.

Permanent complete matrices include public-first, catalog-first, implicit catalog,
quoted/multiple/missing namespaces, fixed and polymorphic shadows, mixed targets,
CASE-dead, WHERE false/NULL, LIMIT 0, Simple and P/D/Execute descriptors, static
unknown failures before earlier volatile targets and real subscription rollback.
The two original whole SRF diagnostics are retained unfiltered. Evidence and
matching all-58-header/source O0 epochs are in
`/tmp/dbms-provider-search-path.tD6Q3AdD/`; no old FunctionCall/Append ABI is linked.

Final immutable artifact: `candidate-v6/dbms_main`, SHA-256
`cf9e8a6e211ce768e0a75d0c6ab74621bb40b23d5c1677cd310d21a540049e8a`.
V5 rebuilt every one of the 58 production TUs and stubs from the final public
headers. V6 rebuilt only the evaluator's private callback-site fix, verified
the other 57 source hashes and all header hashes, and relinked matching objects.
The final 14 whole protocol scripts, ten native drivers and four scoped
ASan/UBSan drivers each finish with exit 0. Strict 180006 passes both new whole
scripts and the original two SRF diagnostics. V1--V5 failures, the cold-owner
42883, harness mistakes and actual callback-slot collision 134 are retained;
they are not relabeled as passing epochs. No protocol deadline was changed.
Sanitizers instrument only parser/evaluator/DDL undo, with leak detection off.
This is private O0 evidence, not normal O2 or the full canonical suite.

New public accessor: `ExprEvaluator::queryHostSetReturningRole(call, engine)` is
metadata-only. This is not all function resolution: arbitrary overloaded/default
signatures, domains/user casts, catalog routine creation, named/default scalar
arguments, SQL-syntax vs ordinary `make_interval` shadowing and broader routine
DDL remain open. No security/TDE, push, Actions or full canonical gate is implied.
