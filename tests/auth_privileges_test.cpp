#include "parser/parser.h"
#include <cassert>
#include <iostream>

static void test_grant_revoke_parser() {
    dbms::SQLParser parser;
    auto r1 = parser.parse("GRANT SELECT, INSERT ON table1 TO user1");
    assert(r1.success);
    auto r2 = parser.parse("GRANT ALL PRIVILEGES ON DATABASE db1 TO admin WITH GRANT OPTION");
    assert(r2.success);
    auto r3 = parser.parse("REVOKE SELECT ON table1 FROM user1");
    assert(r3.success);
    std::cout << "[AUTH] GRANT/REVOKE parse OK" << std::endl;
}

static void test_alter_role_full() {
    dbms::SQLParser parser;
    auto r1 = parser.parse("ALTER ROLE admin CREATEDB CREATEROLE");
    assert(r1.success);
    auto r2 = parser.parse("ALTER USER bob WITH PASSWORD 'secret' VALID UNTIL '2026-12-31'");
    assert(r2.success);
    std::cout << "[AUTH] ALTER ROLE/USER parse OK" << std::endl;
}

int main() {
    test_grant_revoke_parser();
    test_alter_role_full();
    std::cout << "[AUTH_PRIV] all passed" << std::endl;
    return 0;
}
