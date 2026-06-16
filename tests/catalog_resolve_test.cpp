// Catalog name resolution / search_path 测试
// test_sources: src/catalog/catalog.cpp src/catalog/oid.cpp src/catalog/systables.cpp

#include "catalog/catalog.h"
#include <cassert>
#include <filesystem>
#include <iostream>

using namespace dbms;

int main() {
    std::string dbPath = "/tmp/dbms_catalog_resolve_test";
    std::filesystem::remove_all(dbPath);

    CatalogManager mgr(dbPath);
    mgr.bootstrapSystemNamespaces();

    Oid publicOid = mgr.createNamespace("public", 10);
    Oid otherOid = mgr.createNamespace("otherschema", 10);

    PgClassRow t1;
    t1.relname = "t1";
    t1.relnamespace = publicOid;
    t1.relkind = 'r';
    Oid t1oid = mgr.createClass(t1);

    PgClassRow t1Other;
    t1Other.relname = "t1";
    t1Other.relnamespace = otherOid;
    t1Other.relkind = 'r';
    Oid t1OtherOid = mgr.createClass(t1Other);

    PgAttributeRow col;
    col.attrelid = t1oid;
    col.attname = "id";
    col.attnum = 1;
    mgr.addAttribute(col);

    // parseQualifiedName
    {
        CatalogManager::QualifiedName qn;
        assert(CatalogManager::parseQualifiedName("t1", qn));
        assert(qn.schema.empty() && qn.name == "t1");
        assert(CatalogManager::parseQualifiedName("otherschema.t1", qn));
        assert(qn.schema == "otherschema" && qn.name == "t1");
        std::cout << "[CATALOG] parseQualifiedName OK\n";
    }

    // parseSearchPath
    {
        auto sp = CatalogManager::parseSearchPath("public, pg_catalog");
        assert(sp.size() == 2 && sp[0] == "public" && sp[1] == "pg_catalog");
        auto empty = CatalogManager::parseSearchPath("");
        assert(empty.size() == 1 && empty[0] == "public");
        std::cout << "[CATALOG] parseSearchPath OK\n";
    }

    // resolveRelation with search_path
    {
        auto sp = std::vector<std::string>{"public", "otherschema"};
        auto* r = mgr.resolveRelation("t1", sp);
        assert(r && r->oid == t1oid);

        auto* r2 = mgr.resolveRelation("otherschema.t1", {});
        assert(r2 && r2->oid == t1OtherOid);

        auto* r3 = mgr.resolveRelation("nonexistent", sp);
        assert(!r3);
        std::cout << "[CATALOG] resolveRelation OK\n";
    }

    // resolveAttribute
    {
        auto sp = std::vector<std::string>{"public"};
        auto* a = mgr.resolveAttribute("t1", "id", sp);
        assert(a && a->attname == "id");
        auto* a2 = mgr.resolveAttribute("public.t1", "id", {});
        assert(a2 && a2->attname == "id");
        std::cout << "[CATALOG] resolveAttribute OK\n";
    }

    std::filesystem::remove_all(dbPath);
    std::cout << "[CATALOG] all passed\n";
    return 0;
}
