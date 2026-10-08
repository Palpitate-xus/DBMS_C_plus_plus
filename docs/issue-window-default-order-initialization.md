# Window frontend default sort direction initialization

## Actual defect and single-issue repair

The real frontend declares `WindowFunc wf;`. Its `orderByAsc` member had
no initializer. `parseWindowFunc` only assigns that member inside the
nonempty `ORDER BY` branch; successful unordered aggregates, partitioned
ranking functions and named windows therefore leave it indeterminate.
`convertToVolcanoWindowSpec` unconditionally reads it to set
`WindowFunctionSpec::orderAscending`. This is an uninitialized-read defect,
even when an unordered execution subsequently ignores the sort direction.

The production change is solely `bool orderByAsc = true;` in Main's private
frontend structure. It does not invent an ORDER BY key or change explicit
ASC/DESC, NULL placement, frame parsing, partitioning, physical metadata,
public headers or execution operators. QRY-08 remains partial: this is not
proof of every named/inherited window, frame, exclusion, interaction or spill.

## Permanent real frontend regression

`tests/frontend/window_default_order.cpp` includes the actual Main translation
unit and calls its actual parser and Volcano conversion functions. It links
the 57 ordinary production objects without any test stubs. Its separate
`scripts/test_window_default_order.sh` runner is explicitly invoked by
`scripts/build_tests.sh`; it is deliberately outside the ordinary native glob
because including real Main and shared stubs would duplicate global owners.

The fixture default-initializes the real object on both zero-filled and
0xa5-filled aligned storage, just like the actual default-construction site.
It checks the bool's object representation against an initialized true bool
before any unordered conversion, so the negative baseline itself does not
evaluate an indeterminate bool. It retains all 58 checks: default construction,
five aggregate kinds with empty/partitioned/framed/named OVER, three actual
partitioned ranking functions, and explicit ascending/descending/NULL-order
controls. There is no copied parser, source-pattern assertion or mock planner.

## Immutable actual evidence and approval gate

Artifacts: `/tmp/dbms-root-window-default.QUHNQN7A/`.
Base: clean ROOT `b4e9fef4`, production source `29f90184`.

The unchanged actual Main baseline, compiled with normal O2 flags and all
57 individually source/header/flags/manifest/original58-receipt/object-byte
proved current normal production objects, exits1: **58 checks,48 failures**.
Both construction checks and all46 unordered conversion cases fail;
all10 explicit ordering cases pass. `baseline.log` retains every failure.
The initial author baseline compilation mixed absolute Root headers and
relative private headers, producing duplicate declarations; that compiler
failure remains in `verification.log` and is not a DBMS runtime baseline.
The corrected baseline compiles within the actual Root header namespace.

Five complete native neighbours also actually exit0 on default disk with
fresh drivers/stubs: window functions, GROUPS unbounded72, NULL ordering144,
query metadata and full Volcano phase5.1. `native-neighbours.log` retains
every complete invocation, not a handpicked subset of assertions.

The candidate normal Main build/repeat actually completes with exactly one
fresh Main plus57 separately proved current normal objects, **not** fresh58.
Its full five window/aggregate protocol neighbours also actually exit0,
including NULL ordering195 and GROUPS unbounded147. All58 normal source,
header, compiler, flags, receipt and cache identities are independently checked.
Frozen actual normal SHA256:
`0d6c4461b39898d30dd56c3437d1e39a55aa54df319baa0a5e7ea136da77b957`.

The first actual candidate frontend runner compilation succeeded but startup
failed before any test control because real Main requires an explicit data
directory. The original full `verification-v2.log` ends1 and is retained,
not called frontend PASS. The runner now supplies `DBMS_DATA_DIR=.` only
within the helper's owned isolated cwd, just as the genuine negative baseline
already did. No global initialization or data-directory requirement is bypassed.
The unchanged compiled real frontend58 is separately exercised through that
same actual Bash isolation helper with the explicit directory. A mistaken
interactive Zsh invocation of Bash-only build helpers is also not a test PASS.
The exact corrected registered runner now actually completes its formal
normal/frontend gates: session22632 terminal0, real frontend58 failures0,
normal/repeat zero fresh CPP after all58 matching donor proofs/migration,
all58 own path-sensitive receipts/cache and complete source/script/test/manifest
input seal unchanged. Corrected artifacts:
`/tmp/dbms-root-window-default-corrected.KtCPUXHv/`.
Its binary is byte-identical to the already tested normal0d above.
Publication/direct actual main results will be recorded in the canonical
checkpoint; they are not inferred from these private tests.

Separately, current INTEGER composition has complete99 native=0 but its
complete88 protocol gate actually exits1:87 passes and one socket timeout in
quantified demand. That failed gate is preserved; subsequent smaller gates
cannot replace it. None of the INTEGER issues is approved by this window fix.

Original273 requirements/statuses remain unchanged:22 complete,166 partial,
70 unverified,15 deferred by user. No push, Actions activation, skipped security
work, filtered-branch restart, sanitizer/full-suite or overall completion claim.
