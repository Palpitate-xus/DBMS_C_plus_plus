# STO-04: Buffer invalidation must preserve outstanding pins

## Finding and reproduction

`BufferPool::fetchPage()` returns a pointer into a frame that remains owned by
the caller until `unpinPage()`. `invalidateAll()` drained in-flight loads and
previously orphaned frames, but then cleared every frame's page identity and
pin count unconditionally. With a one-frame pool, a caller could keep page 1
pinned, call `invalidateAll()`, then fetch page 2; the clock pool immediately
reused the still-referenced memory. The old caller's later `unpinPage(1)` could
also release the new page's pin because unpin routing is keyed by page id.

`tests/buffer_pool_invalidate_all_pins_test.cpp` reproduces this deterministically:
after persisting and pinning page 1, `invalidateAll()` must leave a one-frame
pool unable to fetch page 2 until page 1's old pin is released. Before the fix,
the assertion failed because page 2 was returned immediately.

## Fix

While holding the buffer metadata mutex, `invalidateAll()` now transfers each
mapped frame's outstanding pin count to `orphanedPins_`, marks the frame as
`kOrphanedPage`, and excludes it from both free-frame selection and eviction.
The final unpin reclaims the frame through the existing orphan path. Unpinned
frames are invalidated as before; in-flight loads and pre-existing orphans are
still drained first.

## Verification

- The regression failed on the old implementation with exit 134 at the
  expected `fetchPage(2) == nullptr` assertion, then passed after the fix.
- `bash scripts/build.sh` passed (this environment builds the TLS stub because
  OpenSSL is unavailable).
- `bash scripts/build_tests.sh` ran 461 C++ tests successfully and 202 of 203
  E2E/protocol entries successfully. The single failure was
  `div14_feature_gate_test.py`, where the test server closed the connection
  during `ALTER TABLE t6e ADD COLUMN later INTEGER UNSIGNED`. A standalone
  rerun immediately passed. The full script therefore remains recorded as
  exit 1, not a full-suite pass.
- No PostgreSQL 18.6 runtime differential was run.

## Remaining STO-04 work

This closes only the `invalidateAll()` pin-lifetime defect. BufferPool still
uses one metadata mutex and raw pointers; it has no buffer content locks,
prefetch API, bulk strategy/ring buffer, or automatic ResourceOwner/RAII pin
cleanup. The in-flight load coordination is process-local. STO-04 remains
partial.
