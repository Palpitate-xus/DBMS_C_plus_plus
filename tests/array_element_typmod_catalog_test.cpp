#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <map>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "array_element_typmod_catalog";
    cleanupTestDb(name);
    const auto database = testDbPath(name);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    session.pid = 942942;
    dbms::setCurrentSession(&session);
    dbms::DdlExecutor ddl;
    const auto check = [&](const std::string& table, bool temporary,
                           const std::map<std::string, int>& modifiers) {
        auto& catalog = g_engine.catalogService().get(database);
        const auto* ns = temporary ? catalog.findTempNamespace(session.pid)
                                  : catalog.findNamespaceByName("public");
        assert(ns);
        const auto* relation = catalog.findClassByName(table, ns->oid);
        assert(relation);
        for (const auto& [column, modifier] : modifiers) {
            const auto* attr = catalog.findAttribute(relation->oid, column);
            assert(attr && attr->attndims == 1);
            if (attr->atttypmod != modifier)
                std::cerr << table << '.' << column << " typmod "
                          << attr->atttypmod << " expected " << modifier << '\n';
            assert(attr->atttypmod == modifier);
        }
    };
    const std::map<std::string, int> initial = {
        {"v", 8}, {"c", 7}, {"n", 393222}, {"z", 393220},
        {"u", -1}, {"dc", 5}, {"dn", -1}, {"maximum", 65539}};
    for (bool temporary : {false, true}) {
        const std::string table = temporary ? "temp_mods" : "mods";
        assert(!ddl.executeSql(std::string("CREATE ") + (temporary ? "TEMP " : "") +
            "TABLE " + table + "(v VARCHAR(4)[],c CHAR(3)[],n NUMERIC(6,2)[],"
            "z NUMERIC(6,0)[],u VARCHAR[],dc CHAR[],dn NUMERIC[],maximum VARCHAR(65535)[])", session));
        check(table, temporary, initial);
        assert(!ddl.executeSql("ALTER TABLE " + table + " ADD COLUMN unrelated INT", session));
        check(table, temporary, initial);
        assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
        assert(g_engine.savepoint("before_mods") == dbms::DBStatus::OK);
        assert(!ddl.executeSql("ALTER TABLE " + table + " ALTER COLUMN v TYPE VARCHAR(7)[]", session));
        assert(!ddl.executeSql("ALTER TABLE " + table + " ALTER COLUMN n TYPE NUMERIC(8,3)[]", session));
        check(table, temporary, {{"v", 11}, {"n", 524295}, {"z", 393220}});
        assert(g_engine.rollbackToSavepoint("before_mods") == dbms::DBStatus::OK);
        check(table, temporary, initial);
        assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
        check(table, temporary, initial);
    }
    assert(g_engine.catalogService().persistAll());
    g_engine.catalogService().evict(database);
    check("mods", false, initial);
    check("temp_mods", true, initial);
    assert(!ddl.executeSql("DROP TABLE mods", session));
    assert(!ddl.executeSql("DROP TABLE temp_mods", session));
    assert(g_engine.dropSessionTemporaryObjects(database, session.pid));
    dbms::setCurrentSession(nullptr);
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[ARRAY ELEMENT TYPMOD CATALOG] passed\n";
}
