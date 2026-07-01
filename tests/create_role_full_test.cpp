#include "parser/parser.h"
#include <cassert>
#include <iostream>

static void test_create_role_with_attrs() {
    dbms::SQLParser parser;
    auto r = parser.parse(
        "CREATE ROLE admin WITH SUPERUSER CREATEDB LOGIN PASSWORD 'secret' CONNECTION LIMIT 100");
    assert(r.success);
    auto* stmt = dynamic_cast<dbms::CreateRoleStmt*>(r.stmt.get());
    assert(stmt);
    assert(stmt->roleName == "admin");
    assert(stmt->superuser);
    assert(stmt->createdb);
    assert(stmt->login);
    assert(stmt->password == "secret");
    assert(stmt->connectionLimit == 100);
    std::cout << "[ROLE] full attrs OK" << std::endl;
}

static void test_create_user_nologin() {
    dbms::SQLParser parser;
    auto r = parser.parse("CREATE USER bob WITH NOLOGIN NOSUPERUSER");
    assert(r.success);
    auto* stmt = dynamic_cast<dbms::CreateRoleStmt*>(r.stmt.get());
    assert(stmt);
    assert(stmt->roleName == "bob");
    assert(stmt->isUser);
    assert(!stmt->login);
    assert(!stmt->superuser);
    std::cout << "[ROLE] user nologin OK" << std::endl;
}

static void test_create_role_in_role() {
    dbms::SQLParser parser;
    auto r = parser.parse("CREATE ROLE u1 IN ROLE role1, role2 LOGIN");
    assert(r.success);
    auto* stmt = dynamic_cast<dbms::CreateRoleStmt*>(r.stmt.get());
    assert(stmt);
    assert(stmt->roleName == "u1");
    assert(stmt->inRole.size() == 2);
    assert(stmt->inRole[0].first == "role1");
    assert(stmt->inRole[1].first == "role2");
    assert(stmt->login);
    std::cout << "[ROLE] in role OK" << std::endl;
}

static void test_valid_until() {
    dbms::SQLParser parser;
    auto r = parser.parse("CREATE ROLE tmp WITH LOGIN VALID UNTIL '2026-12-31'");
    assert(r.success);
    auto* stmt = dynamic_cast<dbms::CreateRoleStmt*>(r.stmt.get());
    assert(stmt);
    assert(stmt->validUntil == "2026-12-31");
    std::cout << "[ROLE] valid until OK" << std::endl;
}

int main() {
    test_create_role_with_attrs();
    test_create_user_nologin();
    test_create_role_in_role();
    test_valid_until();
    std::cout << "[ROLE] all passed" << std::endl;
    return 0;
}
