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
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "bit_type_modifier_catalog";
    cleanupTestDb(name);
    const auto database = testDbPath(name);
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    auto* previous = currentSession();
    setCurrentSession(&session);
    struct Restore { Session* value; ~Restore() { setCurrentSession(value); } } restore{previous};
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE mods(f BIT(5),v VARBIT(7),a BIT(3)[],u VARBIT[],cap VARBIT(8388608),b BIT)", session));
    const std::map<std::string, int32_t> initial = {
        {"f", 5}, {"v", 7}, {"a", 3}, {"u", -1}, {"cap", 8388608}, {"b", 1}};
    size_t controls = 0;
    const auto check = [&](StorageEngine& engine, const std::map<std::string, int32_t>& expected) {
        const auto catalog = engine.catalogService().acquire(database);
        const auto relation = catalog->resolveRelationSnapshot("mods", {"public"});
        assert(relation);
        const auto attributes = catalog->findAttributes(relation->oid);
        for (const auto& [name, modifier] : expected) {
            const auto found = std::find_if(attributes.begin(), attributes.end(),
                [&](const auto& attribute) { return attribute.attname == name; });
            assert(found != attributes.end());
            if (found->atttypmod != modifier)
                std::cerr << name << " modifier " << found->atttypmod << " expected " << modifier << '\n';
            assert(found->atttypmod == modifier);
            assert(found->attndims == (name == "a" || name == "u" ? 1 : 0));
            ++controls;
        }
    };
    check(g_engine, initial);
    assert(!ddl.executeSql("ALTER TABLE mods ADD COLUMN unrelated INT", session));
    check(g_engine, initial);
    assert(g_engine.catalogService().persistAll());
    g_engine.catalogService().evict(database);
    check(g_engine, initial);
    {
        StorageEngine cold;
        check(cold, initial);
    }
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    assert(g_engine.savepoint("before_modifiers") == DBStatus::OK);
    assert(!ddl.executeSql("ALTER TABLE mods ALTER COLUMN f TYPE BIT(9)", session));
    assert(!ddl.executeSql("ALTER TABLE mods ALTER COLUMN v TYPE VARBIT(11)", session));
    auto changed = initial;
    changed["f"] = 9;
    changed["v"] = 11;
    check(g_engine, changed);
    assert(g_engine.rollbackToSavepoint("before_modifiers") == DBStatus::OK);
    check(g_engine, initial);
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    check(g_engine, initial);
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[BIT TYPE MODIFIER CATALOG] " << controls << " declaration/cold/rollback controls passed\n";
}
