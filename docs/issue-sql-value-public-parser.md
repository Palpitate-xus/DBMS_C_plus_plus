# Preserve public parse-result grammar errors for SQL-value keywords

Independent external native diagnosis shows four illegal keyword forms throw
DbError42601 directly from SQLParser::parse; valid bare/precision forms succeed.
The strengthened original keyword native fixture keeps all31 checks and adds
18 assertions for the existing nine invalid statements through public parse
and parseForBinding. On the preceding actual own objects27197 exits134 with an
uncaught42601, retained in sql-value-parser-api-original-baseline.log.

The SQL-value grammar branch now routes syntax errors through the parser's
existing statement-owned retainDeclarationSyntaxError mechanism. It returns a
failed ParseResult with42601, error text and no AST. It does not broaden the
outer parser catch, rewrite invalid SQL as valid function calls, swallow semantic
errors or alter standalone expression parsing without a statement error owner.
Prepared-query rejection and wire SQLSTATEs must remain unchanged.

No public header changes; own affected parser CPP20525 exits0, normal builder
relinks current own objects and repeat does not recompile. Expanded88485 is
terminal1:33/33native and21/23wire pass. Strengthened native49 passes, as do
strict arity50, original function_result and all other original neighbors.
Original BIT384/56735 against actual verified18.6 also exits0. All58 current own
receipts/cache/repeat/source/frozen terminal checks pass; no borrowed objects or
fresh58 claim. Scoped public-result contract repair is independently committed.

Five original NAME creator/dependent failures remain unchanged. Default protocol
times out in its original prepared_transaction_error_boundaries call at CREATE
TABLE deferred_child; retain the whole log and unchanged deadlines. Neither
failed entry is relaxed or investigated as a new filtered branch. Whole gate
and broader grammar/SQL-value family remain open.

Frozen:dbms_main.sql-value-parser-api-initial.frozen.
SHA256:beb50f30e23cf2a3ddc12e6aaf2a83bfcf23fc9f451287667e968c4920eb97fb.
Source seal:a85e653e42b8a466780138e3ae9faf001db78874419dc1cbd8f455f949b8350a.
Log:sql-value-parser-api-initial-gates.log.
Earlier arity gate85145 is terminal1:33native all0/21of23wire pass, arity50 all0;
the five user-filtered NAME failures and default TEMP CTAS timeout are retained.
This is an ordinary parser API repair, not a filtered CREATE/TEMP investigation.

Artifacts:/tmp/dbms-having-grammar.MPk64yc0.
Master full18383 has now ended1 on unchanged795479/02c69, with its terminal
fences; all original failures remain. No new Root source proof or import yet.
Original273 statuses/hash unchanged. No push or Actions activation.
