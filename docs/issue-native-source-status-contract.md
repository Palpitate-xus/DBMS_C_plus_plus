# Native source mutation status compatibility

The new genuine UPDATE source carrier throws DbError when the storage writer
returns a failure status, while the public tryDmlBridge source adapter previously
reported those particular failures through its boolean error return. The
original dml_semantics fixture retains that contract for unique-constraint
failure and exact implicit/parent rollback images. This is an API boundary
compatibility correction, independent of native DELETE USING ownership.

A private DbError subtype distinguishes a writer's returned DBStatus from an
exception raised by an expression or routine. The public UPDATE-source bridge
adapts only that subtype to its historical boolean return after the atomic
owner has rolled back. Its original SQLSTATE remains in the diagnostic. True
expression/routine exceptions, including deliberate errors with the same
SQLSTATE as a constraint, still unwind with their original identity. Other
prepared hosts catch the unchanged DbError base and retain ordinary SQL errors.
No diagnostic text is parsed to classify an error, and no public header/layout,
storage, snapshot or generation state changes.

The independent exact status fixture has two retained failures/exit134 against
the matching old source carrier: implicit and parent calls throw 23505 instead
of returning true, while rollback images remain intact. With the adaptation
both calls return true/handled, preserve all rows and prior parent writes, and
the full fixture exits0. The original SQL and inputs are unchanged.

The combined final full native fixture must prove both constraint status return
and zero effects, and the separate new native source fixtures retain 22012
exception/rollback controls. No intermediate unbuilt commit is called passing.
