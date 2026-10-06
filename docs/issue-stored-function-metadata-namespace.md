# Keep routine type metadata separate from column hints

## Reproduced defect

The ORDER result-type API in `093cf23c` temporarily annotated stored return
types by inserting `\x01routine:<schema>.<function>` keys into the same map
used for column type hints. A quoted column can have that decoded name. Its
BIGINT hint was therefore replaced by the INTEGER return type of `metawriter`.
The inverse lookup could also mistake a caller-supplied column hint for a
builtin function's result type.

The independent pure-metadata native probe actually reproduced the first
collision: `metawriter(1) + "<0x01>routine:.metawriter"` reported `integer`
instead of `bigint` (`31170`, exit 134). The expanded baseline checked the
writing function's table **before** the type assertion: it reported zero
preparation writes and the same incorrect type (`42876`, exit 134). Both
executables use the immutable matching 55-object ORDER/API2 development group
in `/tmp/dbms-function-order-canonical.OHLPmF`; the retained baseline is
`metadata_namespace_baseline.checked`.

## Repair

Routine return types now occupy a separate private map keyed by the actual
FunctionCallExpr node. The metadata prewalk and inference consume the same
live AST; recursive unary/binary/array/CASE/function inference passes that
annotation map explicitly. Column hints remain an independent read-only
namespace. No alternate reserved prefix or guessable SQL identifier is used.

This changes neither public API nor any object/result layout. It still only
reads routine metadata and never invokes a stored function to infer its type.
The regression retains its exact BIGINT assertion and verifies recursive
COALESCE/CASE inference, immunity to a fake builtin-return hint, and zero
preparation writes.

## Retained validation boundary

Candidate artifacts: `/tmp/dbms-function-metadata-namespace.QpsuV6`.
The sole changed helper translation unit rebuilt and linked successfully
(`2673`, exit 0), using the other 54 production objects from the immutable
matching ORDER/API2 group. All headers were unchanged and audited. Source
hashes, compiler output, and binary hash are retained in this directory.
Binary SHA-256:
`3f72db901fd77eb136a29c49d77a13b54d40108a469a7767492299f17060c9ec`.

Ten distinct fresh-linked native tests passed in `4358`: the new metadata
namespace regression, ORDER metadata, scalar resolver, engine owner,
aggregate ambiguity, unchanged constraint expression, statement atomicity,
native PL query host, quoted scalar binding, and function/procedure.
The strengthened regression, including the explicit pre-assertion zero-write
check, and all nine adjacent natives also passed in final repeat `78510`.
Four focused wire tests passed in `14112`: ORDER execution, FROM-less
structured queries, derived types, and table CASE AST.
The unchanged full clause diagnostic still exited 1 with five real failures
(`77518`). This fix does not claim to repair scalar-subquery execution,
EXPLAIN ANALYZE, typed UPDATE, every metadata/type rule, or the ROOT optimized
combination. All related broad review families remain partial.
