# R35: INSERT SELECT must retain ownership of ON CONFLICT

## Defect and fix

`parseInsert` ended a SELECT source only at top-level RETURNING. Consequently
`INSERT INTO t SELECT ... ON CONFLICT ...` sent ON CONFLICT into SELECT binding,
which rejected it with `42601`. Legacy parsing could instead silently leave the
INSERT conflict action empty. The source boundary now stops at a top-level
`ON CONFLICT` pair as well as RETURNING, and resumes the existing INSERT-owned
conflict/RETURNING parser at that exact token. Parenthesized subqueries, JOIN ON,
quoted strings/dollar strings and comments do not acquire INSERT clause roles.
This is a source-ownership correction, not a raw SQL substring replacement.

## Actual evidence

Private worktree `/tmp/dbms-review-root-r35-parser-oct13`, baseline `aeaa3257`.

| Check | Baseline | Candidate / reference |
| --- | --- | --- |
| Native parser/AST fixture, 42 controls in strict and legacy modes | 34 failures, exit 1 | zero failures, exit 0 |
| Focused protocol driver, 17 controls | five mismatches, exit 1 | candidate and PostgreSQL 18.6 exit 0 |
| Whole native required-keyword, conflict binding, INSERT SELECT null bitmap and Boolean-context neighbors | not counted as red reproductions | all four exit 0 |
| Whole protocol required-keyword, INSERT SELECT null bitmap/preflight, INSERT UPDATE RETURNING and PostgreSQL protocol neighbors | not counted as red reproductions | all five exit 0 |

The final native fixture additionally rejects incomplete conflict syntax and
trailing tokens, and verifies conflictAction/selectSource/RETURNING ownership,
not just parse success. An exploratory negative containing `SELECT 1,10 nonsense`
was a valid alias and was corrected to an actually incomplete SET clause before
the accepted baseline/candidate run; its first log is retained.

The protocol fixture checks empty-source and duplicate DO NOTHING, physical
source rows, comment-separated ON CONFLICT, exact values/OIDs, invalid syntax
before sequence execution and metadata-only Describe without nextval execution.
Reference fixtures use unique names and remove only their own tables/sequences.

Logs are `/tmp/dbms-review-root-r35-{native-baseline-final,native-candidate,
focused-baseline,focused-candidate,focused-reference,neighbor-*}.log`.
`r35-parser-proof.sh` verified all 58 immutable donor receipts, headers, compiler
flags, source/TLS manifests and unchanged source bytes. Only parser.cpp was
genuinely recompiled; the other 57 byte-identical objects were remapped after
proof. Native gates checked all current receipts and unchanged final inputs.
Candidate Main SHA-256:
`90be708f935a996ee634ad07882646294bd49abf03a4ac6465a590edbbb46e9b`.

## Independent execution-owner defect: R39 remains open

An additional full 23-control protocol fixture passed PostgreSQL but still had
eight candidate mismatches after this parser fix (ten on the original baseline).
Source DO UPDATE, targeted conflict actions and complex SELECT expressions/JOINs
remain restricted or fall into an invalid legacy receiver. This is not repaired
by changing the source boundary. Every original full-driver SQL and assertion is
preserved in `tests/compat/insert_select_conflict_owner_known_gap.py`; its only
import-context change accommodates the compat directory. The full red logs
`r35-baseline-protocol.log` and `r35-candidate-protocol.log` are retained. It is not
registered as a passing test, and must be made green under R39 before the total
defect list is closed. Neither the focused fixture nor this commit claims those
execution shapes work.

The later [R39 execution-owner fix](issue-review-r39-insert-select-owner.md)
closes this separately. Its registered wrapper runs this same complete fixture,
with no SQL or assertion removed; the original eight-mismatch evidence remains.
