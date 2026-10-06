# Genuine PostgreSQL 18.6 reference and explicit diagnostic modes

## Verified reference, not an inferred image tag

The repository contract remains PostgreSQL **18.6 / server_version_num
180006**. Existing Docker `pgref` actually runs17.2/170002; it was not changed,
recreated or relabelled. Its earlier results remain PG17 diagnostics.

Docker pull `postgres:18.6` session23465 actually exited1 with a registry TCP
timeout. Instead, the official
[18.6 source archive](https://www.postgresql.org/ftp/source/v18.6/) was
downloaded and checked against the publisher's
[SHA256 file](https://ftp.postgresql.org/pub/source/v18.6/postgresql-18.6.tar.bz2.sha256):
`555610c24d53e4316da5b7d3fc25c279d96856d5e0e23ee308c328c5fa881d9f`.
Source, dependency packages, builds, installation and the new cluster are
isolated beneath `/tmp/dbms-pg18-reference.23EK0zXN`.

Missing bison/flex/m4 were extracted from individually SHA256-verified Ubuntu
packages into this prefix only, not installed or upgraded system-wide.
Configure4581, source build82784, install59355, initdb67508 and pg_ctl60336
all actually exited0. The first random-password command failed option parsing
before generating a password; the corrected command produced a mode600
password file. No password is stored in the repository or printed in logs.

The owned server PID624117 listens on127.0.0.1:15486 only, with an owned Unix
socket beneath the same prefix. TCP/SCRAM authentication, actual `version()`,
`SHOW server_version_num` and the unchanged canonical `reference_multi`
version gate all actually pass. Observed version is180006, TimeZone Etc/UTC,
DateStyle ISO/MDY. `runtime-version.log` retains the actual observations.

The actual build profile has **ICU enabled** but **XML, OpenSSL/TLS, LZ4 and
ZSTD absent**, verified in the installed pg_config.h; SSL is off. This profile
is useful for these SQL/type/transaction cases, not evidence for XML, TLS,
compression or all PostgreSQL18 feature families. No deferred security/TDE
work was resumed.

## Explicit release selection and actual gates

Six focused protocol matrices now accept `--reference18`, which calls the
unchanged strict180006 verifier before any fixture mutation. The old
`--reference` still strictly requires170002 and remains labelled PG17.2
diagnostic. Supplying both flags is rejected. No SQL, result, NULL, type OID,
command tag, rollback, sequence-effect or error-priority expectation changed.

Artifacts are `/tmp/dbms-with-cursor-integration.xOfpugBC`:

| Actual verification | Result |
| --- | --- |
| reference-pg18.log | All six reference matrices passed: WITH primary DML47 controls, INSERT width, duplicate UPDATE32 controls, interval INSERT SELECT, whole INSERT SELECT preflight, quoted UPDATE. |
| reference-pg17-legacy-mode.log | The same six matrices passed using their unchanged strict170002 diagnostic mode on the existing reference. |
| reference-sort-pg18.log | All56 original ordinary/plain/TEXT ANALYZE/JSON ANALYZE sort-slot controls passed against actual180006, including distinct genuine sites, correlations and sequence counts. The independent adapter validates actual PostgreSQL JSON shape, not the DBMS-specific renderer shape. |
| Strict wrong-release control | The canonical180006 gate rejects the actually observed170002 reference before fixture mutation. |

Connection variables are PGREF_HOST/PGREF_PORT/PGREF_USER/PGREF_DATABASE and
PGREF_PASSWORD; credentials are loaded from the owned private file, never
embedded in committed commands. These are reference results, not proof that
the new ROOT source combination has passed its own gates or that all273 audit
requirements are complete. Canonical full-suite failures remain retained.
