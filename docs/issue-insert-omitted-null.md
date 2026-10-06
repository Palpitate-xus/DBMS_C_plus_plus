# Complete the final INSERT NEW image

## Reproduced defect and repair

The unchanged `check_add_validation_test` failed in the canonical full run
against source `1e0a8c5a`: inserting only `id` into a table with a nullable
integer `value` and `CHECK(value > 0)` returned `INVALID_VALUE` / `22023`.
The omitted field was absent from the evaluator row, so strict expression
binding treated the known schema column as an unbound name rather than NULL.

After defaults and BEFORE INSERT triggers, `insertInternal` now fills still
absent schema fields with physical SQL NULL before generated expressions and
final constraint checks. It does not overwrite explicit empty text, supplied
values, defaults, or trigger replacements, and does not weaken missing-name
binding. PostgreSQL documents that a CHECK succeeds when its expression is
true or NULL, and generated values are computed after BEFORE triggers:
[constraints](https://www.postgresql.org/docs/18/ddl-constraints.html),
[generated columns](https://www.postgresql.org/docs/18/ddl-generated-columns.html).

## Actual verification

Artifacts: `/tmp/dbms-insert-omitted-null.iHZNTrqu`.

- Original frozen production binary `0204a334...`: new native regression exited
  134; new protocol regression exited 1 with unchanged INSERT SQLSTATE `22023`.
- Candidate compiled the changed TableManage translation unit at normal O2,
  then linked 55 unchanged formal ROOT objects. The verifier audited all 56
  ROOT object signatures, unchanged source hashes, public header hashes and
  the production manifest. This is not claimed as a fresh 56-unit private build.
- Final build session 47180 exited 0. Candidate SHA-256:
  `5502bb01babab381c939a1efe86eacc1b06686863b9fdb9a8b10434d9af41992`.
- Session 97756 exited 0: fresh test/stub compilation and isolated execution of
  11 natives: insert_omitted_null, check_add_validation, check_null_semantics,
  generated_insert_trigger, generated_columns, insert_final_validation,
  insert_final_unique, default_sequence, dml_null_literal_boundary,
  constraint_expr and stored_function_atomicity.
- Protocol session 18981 exited 0: omitted and explicit NULL, rejecting negative
  CHECK values (`23514`), generated NULL operands, default versus explicit NULL,
  empty text versus NULL versus the text `NULL`, physical NULL predicates and
  rollback. The protocol regression is registered in the canonical runner.
- Initial verifier session 58134 exited 2 because the running Bash script was
  edited during execution; its log is retained, not represented as a pass.

This closes the reproduced omission defect only. The canonical 501-native /
244-protocol run on the original ROOT source was still live when these private
proofs finished. Other DML/constraint gaps and the 273-item total remain open;
this focused repair is not full-family or full-suite completion evidence.
