// 临时 schema / 会话隔离测试（catalog 层）
// test_sources: src/catalog/catalog.cpp src/catalog/oid.cpp src/catalog/systables.cpp

#include "catalog/catalog.h"
#include "catalog/systables.h"
#include <cassert>
#include <filesystem>
#include <iostream>

using namespace dbms;

int main() {
    std::string dbPath = "/tmp/dbms_temp_ns_test";
    std::filesystem::remove_all(dbPath);

    CatalogManager mgr(dbPath);
    mgr.bootstrapSystemNamespaces();
    mgr.bootstrapSystemTypes();

    // 两个会话各自创建临时 schema
    Oid ns1 = mgr.createTempNamespace(1001);
    Oid ns2 = mgr.createTempNamespace(1002);
    assert(ns1 != ns2);
    assert(mgr.findTempNamespace(1001)->oid == ns1);
    assert(mgr.findTempNamespace(1002)->oid == ns2);
    std::cout << "[TEMP] create temp namespace OK\n";

    // 在会话 1 的临时 schema 中建表
    PgClassRow tbl;
    tbl.relname = "tt";
    tbl.relnamespace = ns1;
    tbl.relkind = 'r';
    tbl.relnatts = 1;
    Oid tblOid = mgr.createClass(tbl);
    assert(mgr.findClass(tblOid));
    std::cout << "[TEMP] create temp table OK\n";

    // 删除会话 1 的临时 schema，会话 2 不受影响
    assert(mgr.dropTempNamespace(1001));
    assert(mgr.findTempNamespace(1001) == nullptr);
    assert(mgr.findClass(tblOid) == nullptr);
    assert(mgr.findTempNamespace(1002)->oid == ns2);
    std::cout << "[TEMP] drop single temp namespace OK\n";

    // 再次创建会话 1 临时 schema，然后清理全部
    ns1 = mgr.createTempNamespace(1001);
    PgClassRow tbl2;
    tbl2.relname = "tt2";
    tbl2.relnamespace = ns1;
    tbl2.relkind = 'r';
    tbl2.relnatts = 1;
    mgr.createClass(tbl2);

    mgr.dropAllTempNamespaces();
    assert(mgr.findTempNamespace(1001) == nullptr);
    assert(mgr.findTempNamespace(1002) == nullptr);
    std::cout << "[TEMP] drop all temp namespaces OK\n";

    // 幂等性：再次删除不存在的会话不应出错
    assert(!mgr.dropTempNamespace(1001));
    std::cout << "[TEMP] idempotent drop OK\n";

    std::filesystem::remove_all(dbPath);
    std::cout << "[TEMP] all passed\n";
    return 0;
}
