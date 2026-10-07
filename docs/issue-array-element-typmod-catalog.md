# Declared array element modifiers are lost in durable metadata

Independent declaration/catalog repair, following ALTER envelope commit
8639b8f0. It does not repair temporary-range protocol provenance or missing
TIMETZ[] identity; those remain separate issues. It also does not claim array
datum length/precision enforcement or arbitrary ALTER element conversion.

The matched normal-O2 baseline (f6 + original dc7469ea/bbc89ca5) is retained at
`/tmp/dbms-physical-array-typmod.YCjPAKiX/baseline-immutable.udmW5swZ/dbms_main.frozen`
SHA256 `967ed6a358372fceac37c0ddd0c2eb796a3961c02f992fe26b767ab6341f8c0e`.
Its first ordinary-table Simple RowDescription reports -1 for all constrained
array modifiers; element-typmod-focused-baseline.log exits 1. The complete
strict PostgreSQL 18.6/180006 reference passes without changed expectations.
The wider temporary/broad metadata matrix has 70 retained baseline failures.

DDL now records the declaration's VARCHAR/CHAR length or NUMERIC precision
and signed 11-bit scale packing for both arrays and scalars. A physical
numeric column width does not encode precision or scale. Unrelated catalog
synchronization therefore retains its durable modifier; explicit new TYPE
declarations override it. CHAR defaults to length 1; unrestricted VARCHAR
and NUMERIC keep -1. TEMP ALTER TYPE synchronizes the same logical catalog
identity instead of deliberately skipping its attribute update.

Normal-O2 build 35089, repeated build, all 58 source/header/flag/object
signatures and binary stamp passed; candidate SHA256
`4bde5028f5ab44948d35834a743356191f4cdb9e78e5a3a839172271c71844d9`.
New catalog test plus unchanged/new adjacent envelope, TEMP catalog and
catalog-service tests (19251) all passed. The new native controls retain
CREATE, unrelated ADD, maximum VARCHAR capacity, default/unrestricted
modifiers, explicit ALTER modifiers, user savepoint/full rollback and cold
catalog reload for both ordinary and temporary tables.

The focused permanent protocol fixture passed all Simple, Statement Describe
and Portal Describe checks across CREATE/ADD/ALTER/rollback, including empty
results. Its unchanged strict 180006 reference also passed. The original
nine-shape physical-array fixture, table-character CAST Describe and TEMP
ALTER catalog protocol tests passed unchanged. All artifacts/logs are under
`/tmp/dbms-physical-array-typmod.YCjPAKiX/`; default deadlines remain 15 seconds.
TMPDIR=/dev/shm on these semantic tests is not disk-performance closure.
