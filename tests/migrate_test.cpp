// 现有元数据迁移到 Catalog 系统表测试
// 使用默认共享源文件列表（build_tests.sh 已加入 migrate.cpp）

#include "catalog/catalog.h"
#include "catalog/migrate.h"
#include "catalog/systables.h"
#include "commands/TableManage.h"
#include "common/Config.h"
#include "interfaces/table_schema.h"
#include <cassert>
#include <filesystem>
#include <iostream>

using namespace dbms;

dbms::Config g_config;

static Column makeCol(const std::string& name, const std::string& type, size_t size,
                      bool varlen = false) {
    Column c;
    c.dataName = name;
    c.dataType = type;
    c.dsize = size;
    c.isVariableLength = varlen;
    return c;
}

int main() {
    std::string root = "/tmp/dbms_migrate_root";
    std::string catPath = "/tmp/dbms_migrate_catalog";
    std::filesystem::remove_all(root);
    std::filesystem::remove_all(catPath);
    std::filesystem::create_directories(root);
    std::filesystem::current_path(root);

    StorageEngine engine;
    assert(engine.createDatabase("testdb") == DBStatus::OK);

    TableSchema users;
    users.tablename = "app__users";
    users.append(makeCol("id", "int4", 4));
    users.append(makeCol("name", "text", 255, true));
    users.append(makeCol("score", "custom_score", 8)); // 未知类型，需要新建 pg_type
    assert(engine.createTable("testdb", users.tablename, users) == DBStatus::OK);

    TableSchema plain;
    plain.tablename = "plain_table";
    plain.append(makeCol("x", "int4", 4));
    assert(engine.createTable("testdb", plain.tablename, plain) == DBStatus::OK);

    CatalogManager cat(catPath);
    MigrateResult res = migrateDatabaseToCatalog(cat, engine, "testdb");
    assert(res.ok());
    assert(res.tablesMigrated == 2);
    assert(res.namespacesCreated == 1); // app
    assert(res.attributesCreated == 4);
    assert(res.typesCreated == 1);      // custom_score
    std::cout << "[MIGRATE] migrate result OK\n";

    // 验证 namespace
    auto ns = cat.findNamespaceByName("app");
    assert(ns);
    auto publicNs = cat.findNamespaceByName("public");
    assert(publicNs);
    std::cout << "[MIGRATE] namespaces OK\n";

    // 验证 pg_class
    auto cls = cat.findClassByName("users", ns->oid);
    assert(cls);
    assert(cls->relkind == 'r');
    assert(cls->relnatts == 3);

    auto plainCls = cat.findClassByName("plain_table", publicNs->oid);
    assert(plainCls);
    std::cout << "[MIGRATE] pg_class OK\n";

    // 验证 pg_attribute
    auto attrs = cat.findAttributesByNum(cls->oid);
    assert(attrs.size() == 3);
    assert(attrs[0].attname == "id");
    assert(attrs[1].attname == "name");
    assert(attrs[2].attname == "score");
    assert(attrs[0].atttypid == 23); // int4
    assert(attrs[1].atttypid == 25); // text
    assert(attrs[2].atttypid != INVALID_OID);
    std::cout << "[MIGRATE] pg_attribute OK\n";

    // 验证未知类型被创建
    auto custom = cat.findTypeByName("custom_score", ns->oid);
    assert(custom);
    assert(custom->oid == attrs[2].atttypid);
    std::cout << "[MIGRATE] unknown type OK\n";

    // 验证迁移后的持久化
    {
        CatalogManager cat2(catPath);
        assert(cat2.findClassByName("users", cat2.findNamespaceByName("app")->oid));
        std::cout << "[MIGRATE] persistence OK\n";
    }

    std::filesystem::remove_all(root);
    std::filesystem::remove_all(catPath);
    std::cout << "[MIGRATE] all passed\n";
    return 0;
}
