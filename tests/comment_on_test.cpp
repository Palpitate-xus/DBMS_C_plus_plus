// COMMENT ON 对象类型支持测试（catalog 层）
// test_sources: src/catalog/catalog.cpp src/catalog/oid.cpp src/catalog/systables.cpp

#include "catalog/catalog.h"
#include "catalog/systables.h"
#include <cassert>
#include <filesystem>
#include <iostream>

using namespace dbms;

int main() {
    std::string dbPath = "/tmp/dbms_comment_on_test";
    std::filesystem::remove_all(dbPath);

    CatalogManager mgr(dbPath);
    mgr.bootstrapSystemNamespaces();
    mgr.bootstrapSystemTypes();

    Oid publicOid = mgr.createNamespace("public", 10);

    PgProcRow f;
    f.proname = "my_func";
    f.pronamespace = publicOid;
    f.prokind = 'f';
    Oid fid = mgr.createProc(f);

    // SCHEMA comment
    {
        assert(mgr.setComment("SCHEMA", "public", "public schema comment", {}));
        auto ns = mgr.findNamespaceByName("public");
        assert(ns);
        assert(mgr.getDescription(ns->oid, PgClassOid_Namespace, 0) ==
               "public schema comment");
        std::cout << "[COMMENT] schema comment OK\n";
    }

    // TYPE comment (pg_catalog 中的 int4)
    {
        assert(mgr.setComment("TYPE", "pg_catalog.int4", "4-byte integer", {}));
        auto t = mgr.findTypeByName("int4", 11); // pg_catalog = 11
        assert(t);
        assert(mgr.getDescription(t->oid, PgClassOid_Type, 0) == "4-byte integer");
        std::cout << "[COMMENT] type comment OK\n";
    }

    // FUNCTION comment
    {
        assert(mgr.setComment("FUNCTION", "my_func", "my function", {"public"}));
        auto p = mgr.findProc(fid);
        assert(p);
        assert(mgr.getDescription(p->oid, PgClassOid_Proc, 0) == "my function");
        std::cout << "[COMMENT] function comment OK\n";
    }

    // PROCEDURE comment
    {
        PgProcRow p;
        p.proname = "my_proc";
        p.pronamespace = publicOid;
        p.prokind = 'p';
        Oid pid = mgr.createProc(p);
        assert(mgr.setComment("PROCEDURE", "public.my_proc", "my procedure", {}));
        auto pr = mgr.findProc(pid);
        assert(pr);
        assert(mgr.getDescription(pr->oid, PgClassOid_Proc, 0) == "my procedure");
        std::cout << "[COMMENT] procedure comment OK\n";
    }

    // Remove comment (empty string)
    {
        assert(mgr.setComment("SCHEMA", "public", "", {}));
        auto ns = mgr.findNamespaceByName("public");
        assert(ns);
        assert(mgr.getDescription(ns->oid, PgClassOid_Namespace, 0).empty());
        std::cout << "[COMMENT] remove comment OK\n";
    }

    std::filesystem::remove_all(dbPath);
    std::cout << "[COMMENT] all passed\n";
    return 0;
}
