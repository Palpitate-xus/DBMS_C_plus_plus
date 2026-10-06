# Preserve sequence controls when the format and ALTER contract change

This is a test-only adaptation after the independent generation and allocation
position fixes. It changes no production source.

The original `candidate-v3/sequence_full.log` is permanently retained (exit
134). Its descending sequence ALTER expected START 1 to succeed without
changing MAXVALUE -1. The test now retains that exact SQL as a negative
control and verifies the unchanged next value -5 before a separate, valid
explicit NO MINVALUE/NO MAXVALUE restart.

The original bounded sequence SQL still changes INCREMENT 2 to -2 after
allocating its MINVALUE 10. Both original nextval calls remain, now asserting
2200H: they cannot return 12 then 10. The unversioned literal file bytes are
unchanged; after returning 5 and changing increment to -1, the next value is
4, not 6. Strict PostgreSQL 18.6/180006 proves these contracts in the permanent
protocol scripts. Four generated-file expectations now require DBMSSEQ5;
the manually written DBMSSEQ3 backwards-compatibility fixture is unchanged.

The new migration native test supplies exact unversioned, SEQ2, SEQ3 and SEQ4
declarations. It checks readable metadata, upgrade before a real dirty ALTER
rollback image, preserved allocations through savepoint/full rollback, and
exact counter rewind after an explicit physical restore. Its initial draft
asserted migration immediately after BEGIN, before a deferred image existed;
that setup-only exit 134 is retained and is not a production-bug claim.

Matching V4 native evidence:
`/tmp/dbms-sequence-rollback.ORD02uPL/candidate-v4/{sequence_full,
sequence_legacy_generation_migration}.log`, both actual exit 0. The entire
original complete sequence test still runs its other boundary, negative,
ownership, quoted-name, durability and corruption checks; none were removed.

This is not an assertion that all historical is_called information can be
recovered from old formats, all sequence types are PostgreSQL-equivalent,
or that ROOT's formal combination/full differential gate has passed.
