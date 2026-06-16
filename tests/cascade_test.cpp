// CASCADE / RESTRICT 删除计划与执行测试（catalog 层）
// test_sources: src/catalog/catalog.cpp src/catalog/oid.cpp src/catalog/systables.cpp

#include "catalog/catalog.h"
#include "catalog/systables.h"
#include <cassert>
#include <filesystem>
#include <iostream>

using namespace dbms;

int main() {
    std::string dbPath = "/tmp/dbms_cascade_test";
    std::filesystem::remove_all(dbPath);

    CatalogManager mgr(dbPath);
    mgr.bootstrapSystemNamespaces();
    mgr.bootstrapSystemTypes();

    Oid publicOid = mgr.findNamespaceByName("public")->oid;

    // 创建一个 schema，并在其中建表、建类型、建函数
    Oid nsOid = mgr.createNamespace("app", 10);

    PgClassRow tbl;
    tbl.relname = "data";
    tbl.relnamespace = nsOid;
    tbl.relkind = 'r';
    tbl.relnatts = 1;
    Oid tblOid = mgr.createClass(tbl);

    PgTypeRow typ;
    typ.typname = "app_type";
    typ.typnamespace = nsOid;
    typ.typlen = 4;
    typ.typtype = 'b';
    typ.typcategory = 'U';
    Oid typeOid = mgr.createType(typ);

    PgProcRow fn;
    fn.proname = "app_fn";
    fn.pronamespace = nsOid;
    fn.prokind = 'f';
    Oid fnOid = mgr.createProc(fn);

    // RESTRICT 删除 schema 应失败（有依赖对象）
    {
        std::string err;
        bool ok = mgr.dropObject(PgClassOid_Namespace, nsOid,
                                 CatalogManager::DropBehavior::Restrict, &err);
        assert(!ok);
        assert(!err.empty());
        std::cout << "[CASCADE] restrict on schema OK\n";
    }

    // CASCADE 删除 schema 应先删表/类型/函数，再删 schema
    {
        std::string err;
        bool ok = mgr.dropObject(PgClassOid_Namespace, nsOid,
                                 CatalogManager::DropBehavior::Cascade, &err);
        assert(ok);
        assert(err.empty());
        assert(mgr.findNamespace(nsOid) == nullptr);
        assert(mgr.findClass(tblOid) == nullptr);
        assert(mgr.findType(typeOid) == nullptr);
        assert(mgr.findProc(fnOid) == nullptr);
        std::cout << "[CASCADE] cascade on schema OK\n";
    }

    // 手动建立依赖链：public 表 -> public 类型，验证 CASCADE 先删表再删类型
    {
        PgTypeRow t2;
        t2.typname = "base_t";
        t2.typnamespace = publicOid;
        t2.typlen = 4;
        t2.typtype = 'b';
        t2.typcategory = 'U';
        Oid baseOid = mgr.createType(t2);

        PgClassRow t;
        t.relname = "uses_type";
        t.relnamespace = publicOid;
        t.relkind = 'r';
        t.relnatts = 1;
        Oid usesOid = mgr.createClass(t);

        // 手动注册：uses_type 表依赖 base_t 类型
        PgDependRow dep;
        dep.classid = PgClassOid_Class;
        dep.objid = usesOid;
        dep.objsubid = 0;
        dep.refclassid = PgClassOid_Type;
        dep.refobjid = baseOid;
        dep.refobjsubid = 0;
        dep.deptype = 'n';
        mgr.addDepend(dep);

        std::string err;
        assert(!mgr.dropObject(PgClassOid_Type, baseOid,
                               CatalogManager::DropBehavior::Restrict, &err));
        assert(mgr.dropObject(PgClassOid_Type, baseOid,
                              CatalogManager::DropBehavior::Cascade, &err));
        assert(mgr.findType(baseOid) == nullptr);
        assert(mgr.findClass(usesOid) == nullptr);
        std::cout << "[CASCADE] chain dependency OK\n";
    }

    // pin 依赖不可删除
    {
        PgTypeRow pinT;
        pinT.typname = "pinned_t";
        pinT.typnamespace = publicOid;
        pinT.typlen = 4;
        pinT.typtype = 'b';
        pinT.typcategory = 'U';
        Oid pinOid = mgr.createType(pinT);

        PgDependRow pin;
        pin.classid = PgClassOid_Class;
        pin.objid = 9999; // dummy
        pin.objsubid = 0;
        pin.refclassid = PgClassOid_Type;
        pin.refobjid = pinOid;
        pin.refobjsubid = 0;
        pin.deptype = 'p';
        mgr.addDepend(pin);

        std::string err;
        assert(!mgr.dropObject(PgClassOid_Type, pinOid,
                               CatalogManager::DropBehavior::Cascade, &err));
        assert(!err.empty());
        std::cout << "[CASCADE] pin dependency OK\n";
    }

    std::filesystem::remove_all(dbPath);
    std::cout << "[CASCADE] all passed\n";
    return 0;
}
