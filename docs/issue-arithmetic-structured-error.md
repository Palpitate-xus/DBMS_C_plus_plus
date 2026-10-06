# Arithmetic exceptions retain structured SQLSTATE

The scalar helper caught `std::runtime_error` from arithmetic operators and
returned a failed `ExprEvalResult` without `sqlState`. For example, `1/0`
produced a diagnostic containing 22012 but an empty structured error code.
Native PL/pgSQL hosts consequently reported XX000 for a failed RAISE argument.
No runtime diagnostic-text parsing is used to recover a code.

This independent change converts the 28 existing exception creation sites in
`ExprEvaluator::applyArithmetic` to `DbError` with their already specified
SQLSTATE. Operator resolution, arithmetic, range checks and NULL/lazy semantics
are unchanged. `DbError` owns the diagnostic SQLSTATE suffix; a source-constant
follow-up removes the old literal suffix to avoid printing it twice. The existing scalar helper's
`DbError` catch now retains the code. CAST failures have a separate fix;
numeric-power helper errors outside this method and other evaluator/builtin
exception sites are not claimed to be completed by this patch.

The error-code meanings are defined by the official [PostgreSQL 18 error-code
table](https://www.postgresql.org/docs/18/errcodes-appendix.html).

## Retained local evidence

Baseline HEAD `14d9cb2e`: session 74555 exited 134; the first assertion in
`arithmetic_sqlstate_contract_test.cpp` printed `1/0`, empty `sqlState`, and
expected 22012. Separately, RAISE candidate session 93369 exited 134 on the
same native-host propagation issue. Both logs remain under
`/tmp/dbms-plpgsql-raise-state.0N05Nq7X`; the original assertions are retained.

The source-only candidate is retained under
`/tmp/dbms-arithmetic-error-contract.iFAS6Q4K`, binary SHA256
`32ff15e6d7161d71ac56094551152ad469883bfca874034469c1bb1f71975ba6`.
Its 57-source object provenance and unchanged public headers are checked by
`build.sh`; only the evaluator was recompiled with O0. PL runtime uses the
original HEAD object, not the separate pending RAISE patch. This is not a
claim of a new full optimized rebuild.

Sessions 90761 and final-source repeat 63152 exited 0: the new native contract checks 17 exact error-code
controls and three success/NULL/lazy controls; five adjacent native tests
(integer width, floating width, constraint expressions, function atomicity,
and parsed expression metadata) also passed. Session 84345 exited 0 for four
adjacent protocol scripts: integer width, floating width, arithmetic result
types, and stored-function atomicity. These protocol paths were not asserted
to be red before this native error-contract fix; their role is adjacent
regression coverage. The final source differs from the first candidate only
by removal of trailing whitespace, and the binary SHA256 is unchanged.

The READY commit `b91e3d06` and its original evidence are preserved. A separate
tiny follow-up removes only the fixed source-literal SQLSTATE suffixes and
adds an exact single-suffix assertion. This is the same arithmetic root cause,
not a new operator/value fix. The follow-up source was freshly compiled into
the independent RAISE/CAST overlay, binary SHA256
`4e75c97385e8726deff148b8f4ddfa143ba2714cbc62b5996066c0e09eabe2f9`
under `/tmp/dbms-plpgsql-raise-state.0N05Nq7X`. Session 78177 exited 0 for
seven matching natives, including the arithmetic contract and exact diagnostic.
The overlay is explicitly broader than this tiny source-only follow-up; ROOT
combined optimized validation remains separate.

ROOT integration preserves both private commits (`b91e3d06` and `5ddeca6a`)
and combines their source-constant changes into one independent local
arithmetic-root-cause commit. The already separate CAST fix is not replayed.
The integrated ROOT source must still undergo a fresh complete production
build and matching regression run; private proof is not relabeled as proof
of the combined ROOT revision.
