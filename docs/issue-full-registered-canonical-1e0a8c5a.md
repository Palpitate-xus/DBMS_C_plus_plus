# Canonical full run on 1e0a8c5a and independent follow-ups

Session 40539 runs unchanged `scripts/build_tests.sh`: 501 native test sources
and 244 registered protocol/E2E entry points. ROOT source, headers, tests and
registry stay frozen while it is live. The frozen binary SHA-256 is
`0204a334c1f7b8e7534f8dd07a5e4dffb951ae9441ae211a7e012470865f2728`.
All 56 normal O2 production units were freshly built and signature-audited;
the runner's matching 55 no-main objects and fresh stubs were separately
audited. Every native is freshly compiled/linked and runs in isolation.
Log: `/tmp/dbms-prepared-query-namespace-combination.FS4egSBy/full-registered.log`.

Four failures have actually occurred, not just source suspicions:

| Test | Observed result | Follow-up evidence |
| --- | --- | --- |
| check_add_validation | Omitted nullable CHECK column INSERT returned 22023 | Real production omission defect; private fix 229b2b19 passed 11 fresh related natives and dedicated wire |
| integer_bounds | Uncaught BIGINT_MIN / -1 overflow 22003 | Obsolete empty-sentinel test expectation; test-only da8bb245 passed on original ROOT production, preserving precise 22003 and valid modulo |
| geometric | Decimal text not found in V2 binary SP-GiST file | Test-only 2575ba98 passed on original ROOT production, checking exact coordinate bits/RIDs, corruption fallback and fresh-owner reload |
| partial_index | Function-left comparison raised missing column same / 42703 | Fresh native reproducer 19506 exited 134 on original ROOT objects; decoded API RHS identity is lost on scalar fallback; candidate under verification |

The first three commits exist on `fix/insert-omitted-null-iHZNTrqu`, not yet
master. They will be individually integrated after this full gate terminates,
then the combined production will be reverified. Private proofs and issue
notes are in `/tmp/dbms-insert-omitted-null.iHZNTrqu` and its worktree.
They are not ROOT integration or canonical full-suite passes.

Null candidate verification: changed TableManage at normal O2 plus 55 unchanged
formal ROOT objects, with all 56 signatures, unchanged sources, headers and
manifest audited; build 47180, native 97756, wire 18981 exited 0. Candidate SHA
`5502bb01babab381c939a1efe86eacc1b06686863b9fdb9a8b10434d9af41992`.
Original new regression native 90870 exited 134 and wire 82610 exited 1/22023.
Integer correction 71894 and geometric correction 88512 exited 0 on original
ROOT production. Initial geometric fixture wrong RID shift 85756 exited 134;
initial null verifier edited during execution 58134 exited 2. Both logs remain.

The typed scalar carrier independently passed nine natives and seven protocol
scripts on a fresh 57-unit development basis, but ROOT integration and ordinary
consumers are not complete. Typed UPDATE, complete aggregate expressions,
direct WITH scalar queries and typed EXPLAIN remain under implementation and
verification. Original clause failures and full protocol timeout/I/O evidence
remain preserved. No timeout or assertion has been relaxed.

The current full gate is still live; its four existing native failures already
preclude all-green on this frozen revision. Remaining tests must finish to
expose the rest of the integration surface. Totals remain 273: 22 complete,
166 partial, 70 unverified and 15 deferred_by_user. No push, no Actions
enablement and no restart of user-deferred security/TDE work.
