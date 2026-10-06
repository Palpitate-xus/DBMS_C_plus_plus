# Native query exceptions release exactly their own table acquisition

## Reproduction and repair

On original ROOT source `00ce0209` / frozen production `0204a334...`, a native
`query(..., {"typedexpr missing=1"}, ...)` raised the expected `42703` but left
the table lock count behind (before empty, after one). Fresh native baseline
session 91276 exited 134 at the unchanged lock-count assertion.

The structured-output query implementation now uses the existing
`ResourceUnlockGuard` immediately after a successful table acquisition.
Its twelve ad-hoc unlock calls are removed to prevent double release. Normal
returns, metadata/WHERE/ORDER errors and scan/TOAST exits release exactly one
acquisition. It does not release all locks or commit/abort its caller.

## Verification, bounded to this defect

Artifacts: `/tmp/dbms-query-error-lock.w6m5n3`.

- Build session 36425 exited 0: freshly compiled changed TableManage at normal
  O2 plus 55 unchanged formal ROOT objects. All 56 object signatures, unchanged
  source hashes, public headers and manifest were audited; not a fresh private
  56-unit build. Binary SHA-256:
  `27881b8c4cdfb7e8e6ad49b2a6fef5d115ba3e5f43cf46445c4bbf3f7fd3290b`.
- Native session 84290 exited 0: 10 freshly compiled/linked isolated tests.
  The new regression retains precise 42703, 22P02 and 22012, each both without
  a prior lock and with one caller-owned shared lock; complete table count maps
  remain equal before/after, and a subsequent successful query works.
  Adjacent coverage includes existing queryExpr error unwind, FOR UPDATE,
  read-owner indexes, subquery row selection, NULL predicates, compact RHS,
  arithmetic, independent-engine ownership and query snapshot characteristics.
- Wire session 81521 exited 0: row_lock_timeout, for_update_insert_gap,
  transaction_select_table_lock, read_owner_index_snapshot, where_function_scope
  and index_residual_execution_once; deadlines/assertions unchanged.

This is private proof, not yet ROOT integration or full-suite completion.
Canonical ROOT 40539 is still live and has five native plus full-protocol and
table_lock_timeout protocol failures. The latter is an independent implicit
statement transaction start/error propagation defect, not claimed fixed here.
The 273-item total and broad transaction/query families remain incomplete.
