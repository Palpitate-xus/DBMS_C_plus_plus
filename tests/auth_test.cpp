// pg_authid / pg_auth_members CRUD 测试（catalog 层）
// test_sources: src/catalog/catalog.cpp src/catalog/oid.cpp src/catalog/systables.cpp

#include "catalog/catalog.h"
#include "catalog/systables.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>

using namespace dbms;

int main() {
    std::string dbPath = "/tmp/dbms_auth_test";
    std::filesystem::remove_all(dbPath);

    {
        CatalogManager mgr(dbPath);
        mgr.bootstrapSystemNamespaces();
        mgr.bootstrapSystemTypes();

        // CREATE ROLE / USER
        PgAuthIdRow admin;
        admin.rolname = "admin";
        admin.rolsuper = true;
        admin.rolcanlogin = false;
        Oid adminOid = mgr.createAuthId(admin);
        assert(adminOid != INVALID_OID);

        PgAuthIdRow user1;
        user1.rolname = "user1";
        user1.rolcanlogin = true;
        user1.rolconnlimit = 10;
        Oid user1Oid = mgr.createAuthId(user1);
        assert(user1Oid != INVALID_OID);

        // find by OID / name
        assert(mgr.findAuthId(adminOid));
        assert(mgr.findAuthIdByName("admin"));
        assert(mgr.findAuthIdByName("user1"));
        assert(!mgr.findAuthIdByName("nobody"));
        std::cout << "[AUTH] create/find OK\n";

        // UPDATE name
        PgAuthIdRow updated = user1;
        updated.rolname = "user1_renamed";
        updated.rolconnlimit = 20;
        assert(mgr.updateAuthId(user1Oid, updated));
        assert(!mgr.findAuthIdByName("user1"));
        assert(mgr.findAuthIdByName("user1_renamed"));
        assert(mgr.findAuthId(user1Oid)->rolconnlimit == 20);
        std::cout << "[AUTH] update OK\n";

        // LIST
        auto all = mgr.listAuthIds();
        assert(all.size() == 2);
        std::cout << "[AUTH] list OK\n";

        // MEMBERSHIP
        PgAuthMembersRow mem;
        mem.roleid = adminOid;
        mem.member = user1Oid;
        mem.grantor = adminOid;
        mem.admin_option = true;
        mgr.addAuthMember(mem);

        auto members = mgr.findAuthMembers(adminOid);
        assert(members.size() == 1);
        assert(members[0].member == user1Oid);
        auto memberships = mgr.findAuthMemberships(user1Oid);
        assert(memberships.size() == 1);
        assert(memberships[0].roleid == adminOid);
        std::cout << "[AUTH] membership OK\n";

        // REMOVE membership
        assert(mgr.removeAuthMember(adminOid, user1Oid));
        assert(mgr.findAuthMembers(adminOid).empty());
        assert(mgr.findAuthMemberships(user1Oid).empty());
        std::cout << "[AUTH] remove membership OK\n";

        // DROP cascades memberships
        mgr.addAuthMember(mem);
        assert(mgr.dropAuthId(adminOid));
        assert(!mgr.findAuthId(adminOid));
        assert(mgr.findAuthMembers(adminOid).empty());
        assert(mgr.findAuthMemberships(user1Oid).empty());
        std::cout << "[AUTH] drop cascade membership OK\n";
    }

    // Persistence recovery
    {
        CatalogManager mgr(dbPath);
        assert(mgr.findAuthIdByName("admin") == nullptr);
        assert(mgr.findAuthIdByName("user1_renamed"));
        assert(mgr.findAuthMembers(0).empty());
        std::cout << "[AUTH] persistence OK\n";
    }

    std::filesystem::remove_all(dbPath);
    std::cout << "[AUTH] all passed\n";
    return 0;
}
