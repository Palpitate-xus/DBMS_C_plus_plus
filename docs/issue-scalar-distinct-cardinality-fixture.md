# Scalar DISTINCT cardinality fixture must expect actual 21000

This is an independently verified test-oracle correction, not a production
change or a scalar-query family completion. ROOT source104 `af7d46c8`
already reports the PostgreSQL cardinality error21000 for the original query:

```sql
SELECT (SELECT DISTINCT id FROM state_inner) AS value FROM state_outer;
```

The unchanged original setup contains two distinct INTEGER values,1 and2.
DISTINCT does not collapse them to one scalar datum. The old registered
fixture expected unsupported-feature0A000 and was genuinely red in the full
Source80 suite and current OPEN5 gate35004. Those original failures remain
historical; no DBMS result is regressed to0A000 to satisfy that assertion.

The corrected fixture retains all original six SQL statements, empty error
rows, actual SQLSTATE checks, absence of a literal SQLSTATE wrapper in messages,
and each autocommit-error connection-recovery query. Only the sixth expected
code changes from0A000 to21000. A reference-only connection/owned schema adapter
runs those same unqualified statements on verified PostgreSQL18.6/180006.

Additional strong cases retain duplicate1 rows and duplicate SQL NULLs:
one distinct non-NULL or NULL datum succeeds, zero child rows yield SQL NULL,
ordered DISTINCT LIMIT1 succeeds, false/zero parent demand returns no rows
without demanding the two-row child, and the final two-distinct-values query
still reports21000. Success asserts rows/NULLs, header, SELECT tag and OID23.
All failures can be collected while the final invocation remains nonzero.

## Entire actual evidence

Artifacts: `/tmp/dbms-root-distinct-state-fixture.GQO8NwqF/`.

| Complete gate | Terminal result |
| --- | --- |
| Exact current104 original unchanged registered fixture, OPEN5 source35004 | 1; original DISTINCT expectation0A000 versus actual21000 |
| Corrected entire38 controls on owned strict18.6/180006 | 0; original6 statements plus new cardinality/NULL/type/demand/recovery assertions |
| Corrected entire38 on byte-proved current104 frozen binary89255 | 0 |
| Complete six original adjacent whole wrappers57072 | 0; corrected subquery state, scalar cardinality/sort/zero effects, review SQL and FETCH clause |

Actual current104 binary remains SHA256
`3d6446bee02de0ac8ec976cc157b17caf368de452f17891ede6db4eef4f0fc9c`.
No CPP, public header, registry, normal object, execution result, timeout or
original SQL changes. The strict adapter creates only its unique owned schema
and drops that exact schema at exit; no shared public/catalog writes or database
reconfiguration. The engine keeps its ordinary isolated test server.

Other current scalar18/COUNT/UNKNOWN/CTE/FETCH errors and every original273
unclosed requirement remain. This finite fixture correction is not a
full-native/frontend/registered-wire, protocol family or overall approval.
No assistant push, Actions activation or skipped security/TDE work.
