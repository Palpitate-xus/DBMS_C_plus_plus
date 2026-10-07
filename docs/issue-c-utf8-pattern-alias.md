# C.utf8 pattern collation

The same strict PostgreSQL 18.6 control succeeds while exact c465 V1
candidate returns 42704 for `'É' COLLATE "C.utf8" ILIKE 'é'`. Both actual
whole logs are retained in `/tmp/dbms-legacy-sql-pattern.j2twXvZX`.

Accept the existing libc collation name C.utf8. It has binary ordering but
Unicode character classification/case folding, unlike C/POSIX. The native
alias test and the 62-case strict whole verify ILIKE, SIMILAR alpha classes,
`Z < a`, `é > z`, and C negative controls. Final native/whole terminal 0 is
documented in `issue-native-sql-pattern-consumers.md`.

No PostgreSQL OID or catalog row is fabricated. This narrow builtin-name
fix does not assert complete collation/provider-family compatibility.
