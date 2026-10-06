# Row-description relation origins must come from FROM syntax

The old protocol descriptor searched raw lowercased SQL for ` from `. This
confuses literal/comment text with a range source. A projection can consequently
receive an unrelated physical table OID and attribute number, and attempting
that metadata access during a deferred constant-query transaction can acquire
an unnecessary database lock.

## Actual old failure and reference

The isolated old verified `a93312f2` production binary
`/tmp/dbms-query-begin-lock-state.TNJZ551R/dbms_main.lock-state.v3.o2` (SHA-256
`6738292c68369fac3a53970066a52692725c0c1a336bd3ffa96f937507a801d7`)
ran the unchanged new gate at handle `32284`, terminal 1. The direct physical
column positive control had a nonzero relation OID, attribute 1 and INT OID 23.
But `SELECT 'body from descriptor_source tail' AS id` returned literal TEXT
with relation OID 10000 and attribute 1. Correct origin is OID 0 / attribute 0.
Artifact: `/tmp/dbms-extended-literal-demand.zSV7mYvf/literal-origin-baseline.log`.
Owned server 110217 was stopped; the test-owned data directory was cleaned by
the runner.

PostgreSQL 17.2 (`server_version_num=170002`) passed the same literal, comment
and dollar-string controls in Simple Query and Parse/Describe/Sync, plus the
direct physical-column positive. The reference used an owned TEMP relation
and dropped it on connection close. `reference-literal-origin.log` is terminal
0. No reference authentication or roles changed; this is not a PG18.6 suite.

## Fix and scope

The legacy physical-origin helper now reads the parser's actual SELECT FROM
range, canonically decodes a single base source name and excludes visible CTE
sources. A string/comment, expression-grammar FROM, derived query, table
function or set operation cannot become a physical source because it happens
to contain those bytes. No SQL is executed to infer a descriptor.

The dedicated protocol gate retains physical-origin positive assertions and
OID 0 / attribute 0 for literal and comment controls in both result and
prepared descriptions. The extended-owner gate additionally retains a FROM
string under an existing DDL lock; it must return data, not acquire a lock or
terminate the server.

This is a narrow correction to the legacy single-base-source origin helper,
not complete output lineage for joins/derived/CTE/view expressions or a whole
ordinary-query preparation claim. Those broader descriptors remain separate.

## Candidate verification

The helper was tested in the combined private candidate containing the separate
deferred-owner work, not falsely labelled as an exact committed HEAD binary.
V2 full new-header 57-TU official O2 build `56049`, repeat and signature/stamp
audit were terminal 0; its dedicated literal-origin gate passed while its
separate extended gate retained a failure. That failure is not a descriptor
test pass for all transaction paths.

The final V3 official O2 Network-only rebuild/relink `26624`, repeated no-change
build and all 57 source/header/object signature plus stamp checks are terminal
0. Frozen binary `dbms_main.extended.v3.o2` has SHA-256
`cb981f4fffbde28c1c58bb4f41777a0e4073e5b4ea5c469f3d5f939bf3c6933a`.
Protocol group `96051` is terminal 0: the dedicated physical-origin positive
and four literal/comment/dollar-string controls in both Simple and prepared
descriptions, plus the full separate extended-owner gate including FROM text
under a retained DDL lock. Owned server PIDs 250005/252124 are confirmed gone.
Eight matching native gates (`77451`) and ten original adjacent protocol gates
(`14181`) are terminal 0. Later repeat `76031` is retained as terminal 1: the
descriptor fixture timed out its first CREATE TABLE before reaching any origin
assertion; the extended fixture also timed out its first CREATE TABLE. Its CLI
timeout/recovery gate passed. An owned worker was observed in D/submit_bio_wait,
but no complete I/O cause or fix is claimed. A bounded unchanged repeat (`83612`)
is terminal 0 for both full protocol gates; PIDs 266299/266822 are confirmed gone.
These later failures do not erase the actual complete `96051` pass and are not
relabelled as passes. The successful repeat does not close the I/O issue.
