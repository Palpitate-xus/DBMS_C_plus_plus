# HAVING grammar, parser shutdown lifetime and LIMIT error handoff

These repairs are developed in the isolated local worktree
`/tmp/dbms-having-grammar.MPk64yc0/repo`. They are not yet published on master:
the unchanged Root full-suite driver86574 is still live, with its
`src/scripts/tests/cmake` inputs frozen. No push or Actions activation.
The original273 ledger is not complete and its item statuses are unchanged.

## Independent local commits

| Commit | Scoped repair |
| --- | --- |
| `48d35f1d` | Split HAVING conjunctions at actual AND keywords, not the substring in `candy`, a quoted identifier or a literal. |
| `6968d94d` | Identify the real grouped-column comparison AST before aggregate adapters; parentheses in a column name or a literal are not aggregate calls. |
| `cf4e62a0` | Replace heap-backed declaration keyword dictionaries with immutable allocation-free arrays that remain valid during engine shutdown. |
| `e5114a10` | Preserve original invalid LIMIT SQL as a negative parser assertion and add legal ordered FETCH WITH TIES as the positive control. Test-only correction. |
| `75480b5e` | Preserve the actual invalid LIMIT diagnostic in ordinary query dispatch and Extended Parse, before legacy fallback or prepared-statement publication. |

The first three and the final LIMIT handoff are source repairs. Root
remains174 scoped source repairs; the verified and committed candidate has178.
Candidate inventory
is765 automatic native tests +2 actual Main drivers,429 registered protocol
entries and58 production units. These counts are not completion evidence.

## Reproductions and retained failed epochs

| Evidence | Actual result |
| --- | --- |
| HAVING AND identifier baseline / actual PostgreSQL18.6 | Previous frozen candidate18 checks with11 failures; real180006 reference19 checks including BEGIN,zero failures. |
| Parenthesis identity baseline / actual PostgreSQL18.6 | Original53 SQL/assertions retained and extended to68; baseline13 failures, reference69 including BEGIN,zero failures. |
| First HAVING candidate gates64903 | Exit1;18 native pass,25 wire entries contain one failure: literal `a(b)` still admitted by the Main aggregate heuristic. Failed binary/log retained. |
| Corrected Main44404 | Exit0, own-path single-unit rebuild; other57 signatures verified rather than claiming another fresh58 build. |
| Root original domain foreign-key binary | Full driver139; exact same original binary reproduced139 in five independent invocations. Assertions complete, shutdown crashes. |
| Root original test-object backtrace95405 | Exit139; `declaredTypePrefixEligible` -> `consumeDeclaredType` -> `parseTypeSpecification` -> domain ancestry -> schema/cache flush -> StorageEngine destructor -> libc exit. |
| New parser shutdown-lifetime fixture baseline2291 | Exit139, before production fix. New registration runs pure declaration parsing after the function-local dictionary destructor phase. |
| Initial test-only DML correction | Exit134 because the assertion confused diagnostic text with the separate SQLSTATE field. Retained; corrected complete DML50301 is0. Original SQL is not changed into valid SQL. |
| Completed pre-LIMIT candidate22561 | Exit0;22 native and25 wire entries all pass, including the complete original domain-FK/domain-ancestry fixtures, lifetime fixture, original recursive CTE and complete scalar FETCH92. |
| Pre-LIMIT unchanged BIT38464525 | Exit0 against real180006,384 comparisons and zero differences. |
| Original ordinary LIMIT wire probe | Invalid original SQL returned five rows rather than42601; actual PostgreSQL18.6 rejects it. Legal FETCH returns six exact rows/OID23. |
| Expanded LIMIT baseline6651 | Exit1. Extended Parse accepts the invalid SQL without error; ordinary queries also execute rows/volatile effects. Original expectations retained. |
| Expanded LIMIT actual PostgreSQL18.6 reference | Exit0;36 statements including transaction/savepoint controls, genuine Extended Parse42601, exact FETCH rows/OIDs, literal control and sequence effect counts. |
| LIMIT Main66955 / NetworkServer31903 | Both exit0, two actual own-path incremental units; other56 current object signatures verified. |
| Final LIMIT candidate gates5630 | Exit0;22 native and26 wire entries all pass, including the unchanged complete default protocol, original CTE and original quoted-range fixtures. Own58 receipts/cache/repeat/source/frozen fences pass. |
| Final LIMIT unchanged BIT38422258 | Exit0 against real180006;384 comparisons and zero differences. |
| Dedicated final LIMIT fixture69402 | Exit0;17 ordinary statement checks plus genuine Extended Parse42601 and no extra sequence effects. |

Artifacts live under `/tmp/dbms-having-grammar.MPk64yc0`; Root original
backtrace/full-run artifacts are under
`/tmp/dbms-root-catalog-followup.PAz6zkPp`. The diagnostic wrapper links the
actual original Root test object and production objects. Changing its link
order to private objects initially hid the shutdown crash; that non-reproduction
is retained and is not used as repair evidence. No production signal handler,
test-order workaround or recovery-policy change was introduced.

Pre-LIMIT verified frozen binary:
`dbms_main.having-grammar-final.frozen`, SHA256
`34b0b671d4d38edad78609478df9507db4973a6bc61bb9c2e8d71b8e54e75d14`.
Source seal:
`c8f08666af93a0a79e54cded3becb2a16c9c612a753adc577cb8786f53d7fe67`.
Own58 object receipts/cache/no-recompile repeat/source/frozen fences pass.

Final verified LIMIT generation: `dbms_main.limit-handoff-final.frozen`.
SHA256:
`069c9e1bfd2f7d384a25c4a4cd1d878652a5cf587bd1b4b1aa54e91e2d5dbbd7`.
Source seal:
`8d627be9a05c9ce69fe6f604e5082b0509953bd8fff49052a5464d1264ca0c74`.
All previous frozen generations and failed logs remain separate.

## LIMIT handoff and verification plan

1. Preserve the parser's actual `WITH TIES requires FETCH, not LIMIT`
   diagnostic in ordinary Main dispatch, instead of falling through to the
   legacy table executor.
2. Reject the same actual parsed diagnostic in a genuine Extended Parse
   before descriptor fallbacks publish the invalid statement. Do not match
   SQL text, evaluate expressions during Parse, or reject every unsupported
   legacy grammar form indiscriminately.
3. Verify own-path Main and NetworkServer builds, all58 current receipts,
   ordinary/Extended42601, nested scalar errors, legal FETCH peers, quoted
   text, connection reuse and zero extra sequence effects.
4. Run the complete22 native /26 wire candidate matrix, keeping all existing
   SQL/row/OID/count/deadline assertions. Preserve every failed generation.
5. Monitor the existing unchanged full Root driver to its terminal result;
   only then integrate each independently committed candidate onto master
   and perform actual Root-path verification. Private proof is not Root proof.

Root's current whole-run native failures are DML grammar fixture134,
domain-FK shutdown139, quoted enum identity, pattern-cast parser,
stale temporary startup and table-owner atomicity. Its domain-ancestry fixture
passes in this fresh run. Protocol phase is live; the unchanged complete
default protocol has one actual timeout in `extended_query_describe`;
the candidate's unchanged complete default protocol passes in gates5630.
No final Root full-pass/count claim. User-filtered security, temporary/recovery and creator branches are
not reopened. Broader HAVING/grouping, routine graphs, remaining original
whole-run failures and the273 requirements remain open.

README is evergreen and independently committed on master as `aed6e80a`.
