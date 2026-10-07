#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "catalog/CatalogService.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("domain_fk_base");
    assert(g_engine.createDatabase(db,"utf8") == DBStatus::OK);
    Session session; session.currentDB=db; session.username="testuser"; session.permission=1;
    setCurrentSession(&session);
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE DOMAIN fk_parent_domain AS INT NOT NULL CHECK(VALUE > 0)",session));
    assert(!ddl.executeSql("CREATE DOMAIN fk_child_domain AS INT CHECK(VALUE < 10)",session));
    assert(!ddl.executeSql("CREATE TABLE parent(v fk_parent_domain PRIMARY KEY)",session));
    assert(!ddl.executeSql("CREATE TABLE base_parent(v INT PRIMARY KEY)",session));
    assert(g_engine.insert(db,"parent",{{"v","3"}}) == DBStatus::OK);
    assert(g_engine.insert(db,"base_parent",{{"v","3"}}) == DBStatus::OK);
    assert(!ddl.executeSql("CREATE TABLE child(v INT REFERENCES parent(v))",session));
    assert(!ddl.executeSql("CREATE TABLE typed_child(v fk_child_domain REFERENCES parent(v))",session));
    assert(!ddl.executeSql("CREATE TABLE reversed(v fk_parent_domain REFERENCES base_parent(v))",session));
    assert(g_engine.insert(db,"child",{{"v","3"}}) == DBStatus::OK);
    assert(g_engine.insert(db,"child",{{"v","4"}}) == DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.insert(db,"typed_child",{{"v","3"}}) == DBStatus::OK);
    assert(g_engine.insert(db,"typed_child",{{"v","11"}}) == DBStatus::CHECK_VIOLATION);
    assert(g_engine.insertRow(db,"reversed",{{"v",std::nullopt}}) == DBStatus::NULL_NOT_ALLOWED);
    assert(g_engine.insert(db,"reversed",{{"v","-1"}}) == DBStatus::CHECK_VIOLATION);
    assert(g_engine.insert(db,"reversed",{{"v","3"}}) == DBStatus::OK);
    assert(g_engine.getTableSchema(db,"parent").cols[0].domainName == "\"public\".\"fk_parent_domain\"");
    assert(g_engine.getTableSchema(db,"typed_child").cols[0].domainName == "\"public\".\"fk_child_domain\"");
    assert(!g_engine.inTransaction());
    setCurrentSession(nullptr);
    std::cout << "[DOMAIN FOREIGN KEY BASE] identities, CHECK/NULL and comparison compatibility passed\n";
}
