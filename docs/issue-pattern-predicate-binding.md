# Typed pattern predicates and strict ESCAPE inputs

This repairs binding/BOOL output, operator signatures and ESCAPE-input demand
for LIKE/NOT LIKE, ILIKE/NOT ILIKE and SIMILAR TO/NOT SIMILAR TO. It is not a
claim that every Unicode, regular-expression, collation or pattern behavior
is correct.

Whole-query binding resolves the real operand base types before row filtering
or LIMIT. Physical catalog identity survives column, qualified-star, direct
projection and COLLATE binding. Domain ancestry is metadata-only; a physical
array's declared envelope remains an array even when legacy attributes store
its scalar element OID. Invalid operand/escape signatures report 42883 before
execution. Computed/coerced set outputs cannot inherit a different left input's
physical type identity. Parser-owned ESCAPE nodes have genuine BOOL static
types, including execution-owned copies.

Pure root planning preserves strict pattern/escape normalization before lhs
NULL demand, without running routines or child queries to discover NULL.
Text escape length is one Unicode code point, BYTEA one decoded byte. BYTEA
LIKE matches byte units. Actual CHAR lhs padding remains significant; the
right implicit TEXT conversion retains normal CHAR trimming. SELECT and
ordinary DELETE use ROOT's existing retained query/DML execution carrier;
there is no discarded preflight or duplicate DELETE implementation.

## Actual scoped evidence

Earlier full private priority fixtures retained genuine wrong signatures,
ESCAPE priorities and child DELETE failures. The original domain-on-domain
setup was never removed: it required the independent domain foundation now
mapped to ROOT `91060c1b`.

Final current-base candidate: `/tmp/dbms-pattern-domain-current.2UlkbWAe/repo`,
parent `42bd7168`, only these unique pattern changes. All58 private configured
flags with O0, repeat/source/header/object/stamp audits and immutable copy:
handle 78625 exit0, SHA256
`8cc51516e128de44b9e468ef0a3ee2135cf1e5a9ccfdc5ae6b2a35e19c00df42`.

- Fresh matching 11 natives: handle 22766 exit0, `native-v1-11.log`.
- Entire original 11-predicate matrix plus physical-array rejection controls:
  handle 26991 exit0, `whole-pattern-v1-memory-backed.log`; Simple, statement
  and portal BOOL descriptors, NULL, all SELECT/UPDATE/DELETE effects, empty
  relations and unchanged cumulative sequence assertions remain.
- Entire 48-control priority fixture including the original domain-chain
  setup, types/NULL/false/LIMIT/CHAR/BYTEA and genuine correlated ANY/ALL DELETE:
  handle 30710 exit0, `whole-priority-v1-memory-backed.log`.
- Both exact whole reference fixtures pass genuine PostgreSQL 180006.

These two candidate semantic wire runs use memory-backed temporary directories
with the unchanged default deadlines. An earlier c061-based disk-directory
whole run timed out at its repeated INSERT: the failure log remains in
`/tmp/dbms-pattern-current-integration.GQk4jrVC/whole-pattern-v1.log` and is not
retroactively a pass or disk-performance evidence. Current ROOT additionally
has the independent transaction WAL pin; its combined public headers must be
rebuilt from scratch, not mixed with these older-context private objects.

## Required independent remaining runtime repairs

`tests/pattern_unicode_known_gap.py` is an unfiltered, unregistered diagnostic.
All13 strict180006 controls pass; the earlier matching candidate reports11
actual differences: Unicode/newline SIMILAR wildcards, literal regex punctuation,
multibyte SIMILAR ESCAPE, Unicode ILIKE, and a demanded trailing LIKE escape.
The shorter-lhs and NULL-lhs trailing-escape positives remain intact. Logs:
`/tmp/dbms-pattern-current-integration.GQk4jrVC/{reference18-unicode-original.log,candidate-v1-unicode-original.log}`.
These failures are not removed, registered as green, or declared fixed by the
present binding/priority repair. They remain the next pattern-runtime work.
