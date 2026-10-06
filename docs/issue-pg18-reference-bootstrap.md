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

The independently committed EXCLUDED reference mode ea857141 and primitive
unknown-input source fd183ec3 add two further ROOT strict180006 matrices;
reference-excluded-pg18.log and reference-unknown-input-pg18.log actually
exit0. This is now eight selected18 reference matrices plus56 sort controls,
not a canonical full465-case differential run or broad feature-family closure.

## Separate XML-enabled profile

To avoid using an XML-disabled oracle for TYPE-14, a second official18.6
source tree/install is isolated in `/tmp/dbms-pg18-xml-reference.ZWQBlsZ4`.
Nothing in the first running cluster, installed system packages or existing
Docker reference is changed. The same publisher-verified archive supplies
its source. libxml2-dev/libxml2 version2.9.14+dfsg-1.3ubuntu3.9 packages were
downloaded from Ubuntu and individually checked against its package metadata:

- libxml2-dev SHA256 `0884308f010b2401e2c819542f95edc2cc7fd81c25c68d12f750dfb69cfd9ede`.
- libxml2 SHA256 `6e578bc383096718c9eea8a76a3edfacfea06e525e1aa7a187ee41906436d94e`.

The first dev download57204 exited28 (SSL connection timeout); the separately
logged repeat67626 and runtime package3313 actually exited0. Package SHA checks
and local extraction pass. Configure40083 exited0 using isolated headers,
library path, runtime rpath and an owned xml2-config adapter. Its generated
pg_config.h enables USE_LIBXML and USE_ICU. Actual source make84312,
install75069 and initdb77181 exited0. Owned server PID763952 listens on
127.0.0.1:15487. TCP/SCRAM and strict180006 pass. Actual ldd resolves libxml2
to the verified isolated runtime library, not an unverified alternate library.
`runtime-xml.log` has eleven passing controls for document/content, NULL,
XML OID142, XMLSERIALIZE, XMLTABLE and namespace/UTF8 output. This proves the
reference profile, not the DBMS's entire TYPE-14 implementation. The
[PostgreSQL XML type documentation](https://www.postgresql.org/docs/18/datatype-xml.html)
specifies the required libxml build; wider functions are described in the
[XML functions documentation](https://www.postgresql.org/docs/18/functions-xml.html).
Missing TLS/LZ4/ZSTD remain explicit; no skipped security/TDE audit starts.

## Locale must also match the comparison

The first full465-case differential2071 uses this instance's initial C.UTF-8
postgres database. Project collation.cpp explicitly maps `default` to
en_US.UTF-8; historical full compatibility evidence also used en_US.utf8.
Several actual default-string-order differences in that first run therefore
require matched-profile rechecking, not changed rows/goldens or an assumed
production regression. Actual array concat output OID1007/1009 versus25 is
independent of locale and remains a genuine recorded source defect.

A separate owned database `dbms_oracle_en_us_20261006` was created from
template0 on the same XML-enabled instance. Actual pg_database datcollate and
datctype are bothen_US.utf8 and datlocprovider islibc/c; server_version_num is
180006. `runtime-en-us.log` records the observations and the original
`'apple' COLLATE "default" < 'Zoo'` returningtrue. Choose this explicit database
through PGREF_DATABASE for matched full differential verification. The
original C-profile run remains live and will retain its own terminal evidence;
matched full replay is not yet completed or calledgreen. No existing database
locale was changed, dropped or recreated.
