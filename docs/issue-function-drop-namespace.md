# Resolve DROP function identities in their real namespaces

The first qualified-DROP parser candidate changes 0A000 into 42883 but does not
delete the existing routine. Original Gen78 namespace cleanup and its dependent
TEXT-overload controls remain red. DROP20 has9 failures, while actual verified
PostgreSQL18.6/server180006 passes20. Early native11394 additionally shows
`DROP ROUTINE ()` incorrectly accepted as an identifier. Probe4361 isolates
that punctuation case; the other malformed qualified names already fail.

The invalid candidate's expanded46native/28wire driver58725 is deliberately
terminated143 after these reproducible failures, not called a full terminal
verification. Its original logs/frozen3427573 remain. Root full85873 is not
interrupted or changed.

Use a routine identifier grammar for DROP's intact qualified first target.
Resolve the decoded schema/name against actual stored scalar routines in the
session's callable search path, preserving explicit namespace and quoted
boundaries. Pass name and schema separately to existing UDF lookup/removal
APIs and the DDL record. Procedure/TVF lookup currently remains public-only;
never substitute a same-named public object for an unsupported other schema.
Keep the existing signature and multi-target rejection until their actual
identities can be supported.

Own normal parser7204 and DDL71826 compilation exit0. New exact function-
namespace native64099 passes malformed names, explicit deletion, untouched
public sibling, search_path lookup and actual rollback. All original ddl_ast_
bridge, routine_array_signature and Gen cursor natives also pass in that run.
Normal production build and repeat79240 validate all58 current own receipts,
cache and frozen bytes, with no repeat rebuild or copied foreign objects.
Unchanged Gen78 now passes all78 (51734), on the same frozen candidate. The
unchanged DROP20 now has18passes/2failures, solely unsupported qualified CREATE
PROCEDURE and the resulting missing procedure. Preserve both strong positives
for the next independent procedure namespace repair.

Artifacts:/tmp/dbms-having-grammar.MPk64yc0.
Logs:drop-function-namespace-v2-build.log,
drop-function-namespace-v2-generate-series78.log,
drop-function-namespace-v2-drop20.log,
drop-qualified-v2-own-parser-build.log,drop-qualified-v2-own-ddl-build.log.
Frozen:dbms_main.drop-function-namespace-v2.frozen.

The original Gen43native/27wire91737 ends1 with43native/25wire passing,
including complete original default protocol, and all58 terminal fences0.
Qualified DROP is repaired here; the other unchanged fromless pg_typeof failure
also fails on preceding frozen6e6499 and remains ordinary unfinished work.
No new complete default/full-suite claim for this DDL generation. Original273
requirements/statuses and15 explicit security/TDE deferrals remain unchanged.
No push or Actions activation.
