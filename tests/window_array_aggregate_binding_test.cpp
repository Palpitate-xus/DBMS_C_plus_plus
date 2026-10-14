#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <tuple>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "window_array_aggregate_binding";
    cleanupTestDb(name);
    const auto database = testDbPath(name);
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    auto* previous = currentSession();
    setCurrentSession(&session);
    struct Restore { Session* value; ~Restore() { setCurrentSession(value); } } restore{previous};
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE source_rows(i INT,t TEXT,n NUMERIC,b BOOL,a INT[],bits BIT(5),bytes BYTEA)", session));
    size_t controls = 0;
    for (const auto& [argument, type, oid] : std::vector<std::tuple<std::string, std::string, uint32_t>>{
            {"i", "integer[]", 1007}, {"i+1", "integer[]", 1007},
            {"i::bigint", "bigint[]", 1016}, {"t", "text[]", 1009},
            {"n", "numeric[]", 1231}, {"b", "boolean[]", 1000},
            {"CAST(NULL AS INT)", "integer[]", 1007},
            {"bits", "bit[]", 1561}, {"bytes", "bytea[]", 1001}}) {
        for (const auto* suffix : {"", " WHERE false", " LIMIT 0"}) {
            const auto query = g_engine.prepareBoundQuery(database,
                "SELECT pg_catalog.array_agg(" + argument + ") OVER() AS value FROM source_rows" + suffix);
            assert(query.output.size() == 1 && query.output.front().type == type &&
                   query.output.front().typeOid == oid && query.output.front().typeMod == -1);
            ++controls;
        }
    }
    for (const auto& [argument, state] : std::vector<std::pair<std::string, std::string>>{
            {"*", "42883"}, {"NULL", "42725"}, {"'x'", "42725"},
            {"DISTINCT i", "0A000"}, {"missing", "42703"}, {"a", "0A000"}}) {
        std::string actual;
        try { (void)g_engine.prepareBoundQuery(database,
            "SELECT array_agg(" + argument + ") OVER() FROM source_rows WHERE false"); }
        catch (const DbError& error) { actual = error.sqlState(); }
        assert(actual == state);
        ++controls;
    }
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[WINDOW ARRAY AGGREGATE BINDING] " << controls
              << " pure descriptor/empty-source/signature controls passed\n";
}
