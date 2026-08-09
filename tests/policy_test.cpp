#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "expression/expr_helper.h"
#include "Session.h"
#include "catalog/catalog.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "utils/permissions.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_create_policy_all() {
    std::string db = testDbPath("policy_all");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY)", s));
    bool err = ddl.executeSql("CREATE POLICY p ON t", s);
    assert(!err);

    auto policies = g_engine.getPolicies(db, "t");
    assert(policies.size() == 1);
    assert(policies[0].name == "p");
    assert(policies[0].cmd == "ALL");

    cleanup(db);
    std::cout << "[POLICY] ALL OK" << std::endl;
}

static void test_create_policy_using_and_check() {
    std::string db = testDbPath("policy_check");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, owner VARCHAR(50))", s));
    assert(!ddl.executeSql("CREATE POLICY p ON t FOR UPDATE TO public USING (owner = current_user) WITH CHECK (owner = current_user)", s));

    auto policies = g_engine.getPolicies(db, "t");
    assert(policies.size() == 1);
    assert(policies[0].cmd == "UPDATE");
    assert(!policies[0].usingExpr.empty());
    assert(!policies[0].withCheckExpr.empty());

    cleanup(db);
    std::cout << "[POLICY] USING + WITH CHECK OK" << std::endl;
}

static void test_create_policy_insert() {
    std::string db = testDbPath("policy_insert");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY)", s));
    assert(!ddl.executeSql("CREATE POLICY p ON t FOR INSERT WITH CHECK (id > 0)", s));

    auto policies = g_engine.getPolicies(db, "t");
    assert(policies.size() == 1);
    assert(policies[0].cmd == "INSERT");
    assert(policies[0].withCheckExpr.find("id") != std::string::npos);

    cleanup(db);
    std::cout << "[POLICY] INSERT OK" << std::endl;
}

static void test_rls_visible_source_scan() {
    std::string db = testDbPath("policy_runtime");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE target (id INT PRIMARY KEY, val INT)", s));
    assert(!ddl.executeSql("CREATE TABLE source (id INT PRIMARY KEY, owner VARCHAR(50), delta INT)", s));
    assert(g_engine.insert(db, "target", {{"id", "1"}, {"val", "0"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "target", {{"id", "2"}, {"val", "0"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "source", {{"id", "1"}, {"owner", "testuser"}, {"delta", "10"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "source", {{"id", "2"}, {"owner", "otheruser"}, {"delta", "20"}}) == dbms::DBStatus::OK);
    assert(!ddl.executeSql(
        "CREATE POLICY source_select ON source FOR SELECT TO public USING (owner = current_user)", s));
    assert(g_engine.enableRowLevelSecurity(db, "source") == dbms::DBStatus::OK);

    dbms::StorageEngine::setRLSUser(s.username);
    std::string policyError;
    assert(dbms::ExprHelper::evalBool(
        "owner = current_user", {{"owner", "testuser"}},
        {{"owner", "varchar"}}, &policyError, db, s.username));
    size_t visible = 0;
    assert(g_engine.forEachVisibleRow(
        db, "source", "SELECT",
        [&](uint32_t, uint16_t, const char*, size_t) { ++visible; }));
    assert(visible == 1);
    const auto visibleRows = g_engine.query(db, "source", {}, {"id"});
    assert(visibleRows.size() == 1);

    assert(!ddl.executeSql("CREATE TABLE protected_delete (id INT PRIMARY KEY, owner VARCHAR(50))", s));
    assert(g_engine.insert(db, "protected_delete", {{"id", "1"}, {"owner", "testuser"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "protected_delete", {{"id", "2"}, {"owner", "otheruser"}}) == dbms::DBStatus::OK);
    assert(!ddl.executeSql(
        "CREATE POLICY protected_delete_policy ON protected_delete FOR DELETE USING (owner = current_user)", s));
    assert(g_engine.enableRowLevelSecurity(db, "protected_delete") == dbms::DBStatus::OK);
    assert(g_engine.remove(db, "protected_delete", {}) == dbms::DBStatus::OK);
    size_t remaining = 0;
    g_engine.forEachRow(db, "protected_delete", [&](uint32_t, uint16_t, const char*, size_t) { ++remaining; });
    assert(remaining == 1);

    assert(!ddl.executeSql("CREATE TABLE protected_write (id INT PRIMARY KEY, owner VARCHAR(50))", s));
    assert(!ddl.executeSql(
        "CREATE POLICY protected_write_policy ON protected_write FOR ALL "
        "USING (owner = current_user) WITH CHECK (owner = current_user)", s));
    assert(g_engine.enableRowLevelSecurity(db, "protected_write") == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "protected_write", {{"id", "1"}, {"owner", "testuser"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "protected_write", {{"id", "2"}, {"owner", "otheruser"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.update(db, "protected_write", {{"owner", "otheruser"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);

    bool handled = false;
    assert(!dbms::tryDmlBridge(
        "UPDATE target SET val = target.val + s.delta FROM source AS s "
        "WHERE target.id = s.id",
        dbms::SqlCommand::Update, s, handled));
    assert(handled);

    std::vector<std::pair<int, int>> values;
    const auto target = g_engine.getTableSchema(db, "target");
    g_engine.forEachRow(db, "target", [&](uint32_t, uint16_t, const char* data, size_t len) {
        const std::string row(data, len);
        values.emplace_back(std::stoi(g_engine.extractColumnValue(row, target, 0, db, true)),
                            std::stoi(g_engine.extractColumnValue(row, target, 1, db, true)));
    });
    assert(values.size() == 2);
    assert(values[0] == std::make_pair(1, 10));
    assert(values[1] == std::make_pair(2, 0));

    assert(!ddl.executeSql("CREATE TABLE no_policy (id INT PRIMARY KEY)", s));
    assert(g_engine.insert(db, "no_policy", {{"id", "1"}}) == dbms::DBStatus::OK);
    assert(g_engine.enableRowLevelSecurity(db, "no_policy") == dbms::DBStatus::OK);
    visible = 0;
    assert(g_engine.forEachVisibleRow(
        db, "no_policy", "SELECT",
        [&](uint32_t, uint16_t, const char*, size_t) { ++visible; }));
    assert(visible == 0);

    assert(!ddl.executeSql(
        "CREATE TABLE policy_modes (id INT PRIMARY KEY)", s));
    assert(g_engine.insert(db, "policy_modes", {{"id", "1"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "policy_modes", {{"id", "2"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "policy_modes", {{"id", "3"}}) == dbms::DBStatus::OK);
    assert(!ddl.executeSql(
        "CREATE POLICY mode_left ON policy_modes FOR SELECT USING (id <= 2)", s));
    assert(!ddl.executeSql(
        "CREATE POLICY mode_right ON policy_modes FOR SELECT USING (id = 3)", s));
    assert(!ddl.executeSql(
        "CREATE POLICY mode_restrictive ON policy_modes AS RESTRICTIVE FOR SELECT USING (id > 1)", s));
    assert(g_engine.enableRowLevelSecurity(db, "policy_modes") == dbms::DBStatus::OK);
    visible = 0;
    assert(g_engine.forEachVisibleRow(
        db, "policy_modes", "SELECT",
        [&](uint32_t, uint16_t, const char*, size_t) { ++visible; }));
    assert(visible == 2);

    assert(!ddl.executeSql(
        "CREATE TABLE policy_default_check (id INT PRIMARY KEY, owner VARCHAR(50))", s));
    assert(!ddl.executeSql(
        "CREATE POLICY default_check ON policy_default_check FOR ALL "
        "USING (owner = current_user)", s));
    assert(g_engine.enableRowLevelSecurity(db, "policy_default_check") == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "policy_default_check",
                           {{"id", "1"}, {"owner", "testuser"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "policy_default_check",
                           {{"id", "2"}, {"owner", "otheruser"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.update(db, "policy_default_check", {{"owner", "otheruser"}},
                           {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);

    auto& auth = authCatalog();
    const std::string regularName = "rls_regular_policy_test";
    const std::string superName = "rls_super_policy_test";
    const std::string bypassName = "rls_bypass_policy_test";
    const std::string ownerName = "rls_owner_policy_test";
    const std::string inheritedParentName = "rls_inherited_parent_test";
    const std::string noinheritMemberName = "rls_noinherit_member_test";
    const auto ensureRole = [&](const std::string& name, bool superuser, bool bypass,
                                bool inherit = true) {
        auto account = auth.getAuthIdByName(name);
        dbms::PgAuthIdRow row = account.value_or(dbms::PgAuthIdRow{});
        row.rolname = name;
        row.rolsuper = superuser;
        row.rolbypassrls = bypass;
        row.rolinherit = inherit;
        row.rolcanlogin = true;
        if (account) {
            assert(auth.updateAuthId(account->oid, row));
        } else {
            assert(auth.createAuthId(row) != dbms::INVALID_OID);
        }
    };
    ensureRole(regularName, false, false);
    ensureRole(superName, true, false);
    ensureRole(bypassName, false, true);
    ensureRole(ownerName, false, false);
    ensureRole(inheritedParentName, false, false);
    ensureRole(noinheritMemberName, false, false, false);
    Session rolePrivilegeSession;
    rolePrivilegeSession.username = superName;
    rolePrivilegeSession.authenticatedUser = superName;
    rolePrivilegeSession.permission = 1;
    rolePrivilegeSession.authenticatedPermission = 1;
    rolePrivilegeSession.currentRole = regularName;
    assert(!sessionIsAdmin(rolePrivilegeSession));
    rolePrivilegeSession.currentRole.clear();
    assert(sessionIsAdmin(rolePrivilegeSession));
    const auto inheritedParent = auth.getAuthIdByName(inheritedParentName);
    const auto noinheritMember = auth.getAuthIdByName(noinheritMemberName);
    assert(inheritedParent && noinheritMember);
    dbms::PgAuthMembersRow roleMembership;
    roleMembership.roleid = inheritedParent->oid;
    roleMembership.member = noinheritMember->oid;
    roleMembership.grantor = inheritedParent->oid;
    roleMembership.admin_option = false;
    auth.removeAuthMember(inheritedParent->oid, noinheritMember->oid);
    auth.addAuthMember(roleMembership);
    auth.persistAll();

    assert(userIsMemberOfRole(noinheritMemberName, inheritedParentName));
    assert(!userHasRole(noinheritMemberName, inheritedParentName));
    assert(canSetRole(noinheritMemberName, inheritedParentName));
    assert(!hasRoleAdminOption(inheritedParentName, noinheritMemberName));
    assert(grantRoleToUser(inheritedParentName, noinheritMemberName, true,
                           noinheritMemberName) == 0);
    assert(hasRoleAdminOption(inheritedParentName, noinheritMemberName));
    const auto adminRelations = auth.findAuthMemberships(noinheritMember->oid);
    assert(adminRelations.size() == 1);
    assert(adminRelations[0].grantor == noinheritMember->oid);
    assert(grantRoleToUser(inheritedParentName, noinheritMemberName) == -2);
    assert(revokeRoleAdminOption(inheritedParentName, noinheritMemberName));
    assert(!hasRoleAdminOption(inheritedParentName, noinheritMemberName));
    assert(!ddl.executeSql("CREATE TABLE inherited_acl (id INT PRIMARY KEY)", s));
    g_engine.grant(db, "inherited_acl", inheritedParentName,
                   dbms::StorageEngine::TablePrivilege::Select);
    assert(!g_engine.hasPermission(db, "inherited_acl", noinheritMemberName,
                                   dbms::StorageEngine::TablePrivilege::Select));
    assert(!ddl.executeSql(
        "CREATE POLICY inherited_policy ON inherited_acl FOR SELECT TO " +
            inheritedParentName + " USING (true)", s));
    assert(g_engine.getApplicablePolicies(db, "inherited_acl", "SELECT",
                                          noinheritMemberName).empty());

    auto inheritedMember = auth.getAuthIdByName(noinheritMemberName);
    assert(inheritedMember);
    inheritedMember->rolinherit = true;
    assert(auth.updateAuthId(inheritedMember->oid, *inheritedMember));
    auth.persistAll();
    assert(userHasRole(noinheritMemberName, inheritedParentName));
    assert(g_engine.hasPermission(db, "inherited_acl", noinheritMemberName,
                                  dbms::StorageEngine::TablePrivilege::Select));
    assert(g_engine.getApplicablePolicies(db, "inherited_acl", "SELECT",
                                          noinheritMemberName).size() == 1);

    const int ownerTargetMembership = grantRoleToUser(regularName, ownerName);
    assert(ownerTargetMembership == 0 || ownerTargetMembership == -2);

    assert(!ddl.executeSql(
        "CREATE TABLE policy_bypass (id INT PRIMARY KEY)", s));
    assert(g_engine.insert(db, "policy_bypass", {{"id", "1"}}) == dbms::DBStatus::OK);
    assert(!ddl.executeSql(
        "CREATE POLICY deny_all ON policy_bypass FOR SELECT USING (false)", s));
    assert(g_engine.enableRowLevelSecurity(db, "policy_bypass") == dbms::DBStatus::OK);

    visible = 0;
    dbms::StorageEngine::setRLSUser(regularName);
    assert(g_engine.forEachVisibleRow(
        db, "policy_bypass", "SELECT",
        [&](uint32_t, uint16_t, const char*, size_t) { ++visible; }));
    assert(visible == 0);

    dbms::StorageEngine::setRLSUser(superName);
    visible = 0;
    assert(g_engine.forEachVisibleRow(
        db, "policy_bypass", "SELECT",
        [&](uint32_t, uint16_t, const char*, size_t) { ++visible; }));
    assert(visible == 1);

    dbms::StorageEngine::setRLSUser(bypassName);
    visible = 0;
    assert(g_engine.forEachVisibleRow(
        db, "policy_bypass", "SELECT",
        [&](uint32_t, uint16_t, const char*, size_t) { ++visible; }));
    assert(visible == 1);

    assert(g_engine.enableRowLevelSecurity(db, "policy_bypass", true) == dbms::DBStatus::OK);
    dbms::StorageEngine::setRLSUser(superName);
    visible = 0;
    assert(g_engine.forEachVisibleRow(
        db, "policy_bypass", "SELECT",
        [&](uint32_t, uint16_t, const char*, size_t) { ++visible; }));
    assert(visible == 0);

    // A table owner bypasses ordinary RLS, but FORCE ROW LEVEL SECURITY
    // applies policies to the owner as well. The owner must be persisted in
    // the canonical schema and pg_class metadata, not a compatibility sidecar.
    Session ownerSession = s;
    ownerSession.username = ownerName;
    assert(!ddl.executeSql("CREATE TABLE policy_owner (id INT PRIMARY KEY)", ownerSession));
    assert(g_engine.getTableSchema(db, "policy_owner").owner == ownerName);
    assert(g_engine.hasPermission(db, "policy_owner", ownerName,
                                  dbms::StorageEngine::TablePrivilege::Select));
    assert(g_engine.hasColumnPermission(
        db, "policy_owner", ownerName, dbms::StorageEngine::TablePrivilege::Update,
        {"id"}));
    assert(g_engine.hasGrantOption(db, "policy_owner", ownerName,
                                   dbms::StorageEngine::TablePrivilege::Select));
    const auto ownerPermissions = g_engine.getUserPermissions(db, "policy_owner", ownerName);
    assert(ownerPermissions.size() == 1 && ownerPermissions[0] == "all");
    const auto ownerAccount = auth.getAuthIdByName(ownerName);
    const auto ownerRelation = g_engine.catalogService().get(db).resolveRelation("policy_owner", {"public"});
    assert(ownerAccount && ownerRelation && ownerRelation->relowner == ownerAccount->oid);
    assert(g_engine.insert(db, "policy_owner", {{"id", "1"}}) == dbms::DBStatus::OK);
    assert(!ddl.executeSql("CREATE POLICY deny_owner ON policy_owner FOR SELECT USING (false)", ownerSession));
    assert(g_engine.enableRowLevelSecurity(db, "policy_owner") == dbms::DBStatus::OK);

    dbms::StorageEngine::setRLSUser(regularName);
    visible = 0;
    assert(g_engine.forEachVisibleRow(
        db, "policy_owner", "SELECT",
        [&](uint32_t, uint16_t, const char*, size_t) { ++visible; }));
    assert(visible == 0);
    dbms::StorageEngine::setRLSUser(ownerName);
    visible = 0;
    assert(g_engine.forEachVisibleRow(
        db, "policy_owner", "SELECT",
        [&](uint32_t, uint16_t, const char*, size_t) { ++visible; }));
    assert(visible == 1);
    assert(g_engine.enableRowLevelSecurity(db, "policy_owner", true) == dbms::DBStatus::OK);
    visible = 0;
    assert(g_engine.forEachVisibleRow(
        db, "policy_owner", "SELECT",
        [&](uint32_t, uint16_t, const char*, size_t) { ++visible; }));
    assert(visible == 0);

    // GRANT OPTION is a separate ACL bit: removing it keeps the upstream
    // privilege, while CASCADE removes privileges granted downstream.
    g_engine.grant(db, "policy_owner", inheritedParentName,
                   dbms::StorageEngine::TablePrivilege::Select, {}, true, ownerName);
    g_engine.grant(db, "policy_owner", bypassName,
                   dbms::StorageEngine::TablePrivilege::Select, {}, false,
                   inheritedParentName);
    assert(g_engine.hasPermission(
        db, "policy_owner", bypassName,
        dbms::StorageEngine::TablePrivilege::Select));
    assert(g_engine.hasGrantOption(
        db, "policy_owner", inheritedParentName,
        dbms::StorageEngine::TablePrivilege::Select));
    assert(!g_engine.revoke(
        db, "policy_owner", inheritedParentName,
        dbms::StorageEngine::TablePrivilege::Select, {}, false, true));
    assert(g_engine.revoke(
        db, "policy_owner", inheritedParentName,
        dbms::StorageEngine::TablePrivilege::Select, {}, true, true));
    assert(g_engine.hasPermission(
        db, "policy_owner", inheritedParentName,
        dbms::StorageEngine::TablePrivilege::Select));
    assert(!g_engine.hasGrantOption(
        db, "policy_owner", inheritedParentName,
        dbms::StorageEngine::TablePrivilege::Select));
    assert(!g_engine.hasPermission(
        db, "policy_owner", bypassName,
        dbms::StorageEngine::TablePrivilege::Select));

    Session unauthorizedOwnerChange = ownerSession;
    unauthorizedOwnerChange.username = regularName;
    unauthorizedOwnerChange.permission = 1;
    assert(ddl.executeSql("ALTER TABLE policy_owner OWNER TO " + regularName,
                          unauthorizedOwnerChange));
    assert(g_engine.getTableSchema(db, "policy_owner").owner == ownerName);

    Session alterSession = ownerSession;
    assert(!ddl.executeSql("ALTER TABLE policy_owner OWNER TO " + regularName, alterSession));
    assert(g_engine.getTableSchema(db, "policy_owner").owner == regularName);
    dbms::StorageEngine::setRLSUser(regularName);
    visible = 0;
    assert(g_engine.forEachVisibleRow(
        db, "policy_owner", "SELECT",
        [&](uint32_t, uint16_t, const char*, size_t) { ++visible; }));
    assert(visible == 0);
    assert(g_engine.enableRowLevelSecurity(db, "policy_owner") == dbms::DBStatus::OK);
    visible = 0;
    assert(g_engine.forEachVisibleRow(
        db, "policy_owner", "SELECT",
        [&](uint32_t, uint16_t, const char*, size_t) { ++visible; }));
    assert(visible == 1);

    assert(auth.dropAuthId(auth.getAuthIdByName(regularName)->oid));
    assert(auth.dropAuthId(auth.getAuthIdByName(superName)->oid));
    assert(auth.dropAuthId(auth.getAuthIdByName(bypassName)->oid));
    assert(auth.dropAuthId(auth.getAuthIdByName(ownerName)->oid));
    assert(auth.dropAuthId(auth.getAuthIdByName(inheritedParentName)->oid));
    assert(auth.dropAuthId(auth.getAuthIdByName(noinheritMemberName)->oid));
    auth.persistAll();

    dbms::StorageEngine::setRLSUser("");
    cleanup(db);
    std::cout << "[POLICY] runtime visibility and default-deny OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_create_policy_all();
    test_create_policy_using_and_check();
    test_create_policy_insert();
    test_rls_visible_source_scan();
    std::cout << "[POLICY] all passed" << std::endl;
    return 0;
}
